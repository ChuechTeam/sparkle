import PartitionState.*
import WorkerState.{Assigned, Available}
import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.slf4j.{Logger, LoggerFactory}

import java.time.Instant
import scala.concurrent.duration.DurationInt
import scala.util.Random

// Marker trait for Kryo serialization
//trait SerializableMessage


/**
 * A message that a worker can receive.
 */
sealed trait WorkerMessage

/**
 * A message that the master can receive.
 */
sealed trait MasterMessage

case class WorkBatch[T](index: Int, data: Vector[T], stage: Stage[T, ?, ?], master: MasterRef) extends WorkerMessage

case class WorkAccepted(index: Int, worker: WorkerRef) extends MasterMessage

case class WorkDone(index: Int, result: Vector[?], worker: WorkerRef) extends MasterMessage

case class WorkerDied(worker: WorkerRef) extends MasterMessage

case class RetrySendWork(index: Int) extends MasterMessage

// not sure
//case class WorkFailed(errorMessage: String, worker: ActorRef[WorkBatch[?]])

type WorkerRef = ActorRef[WorkerMessage]
type MasterRef = ActorRef[MasterMessage]

val rng = Random()

object Worker {
  private val log: Logger = LoggerFactory.getLogger(Worker.getClass)

  def apply(enableCrashForTest: Boolean): Behavior[WorkerMessage] = Behaviors.receive { (ctx, msg) =>
    msg match {
      case WorkBatch(index, data, stage, master) =>
        println(s"[WORKER] ${ctx.self.path.name} Received partition $index")
        if (enableCrashForTest && rng.nextFloat() <= 0.02) {
          println(s"[WORKER] ${ctx.self.path.name} CRASHING INTENTIONALLY !")
          throw RuntimeException(s"test1: simulated crash on ${ctx.self.path.name}")
        }
        master ! WorkAccepted(index, ctx.self)
        val processed = stage.mapper(data)
        println(s"[WORKER] ${ctx.self.path.name} Finished partition $index")
        master ! WorkDone(index, processed, ctx.self)
        Behaviors.same
    }
  }
}

enum PartitionState {
  case Waiting
  case Dispatched(who: WorkerRef, since: Instant, retryCount: Int)
  case Processing(who: WorkerRef, since: Instant)
  case Done(result: Vector[?])
}

enum WorkerState {
  case Available
  case Assigned(partitionIdx: Int)
}

case class MasterState(partitionStates: Vector[PartitionState],
                       waitingPartitions: Set[Int],
                       workers: Map[WorkerRef, WorkerState],
                       nextWorkerNumber: Int,
                       partitionsDone: Int)

object Master {
  def apply[I, O, F](task: Task[I, O, F], enableCrashForTest: Boolean): Behavior[MasterMessage] =
    Behaviors.setup { context =>
      Behaviors.withTimers { timers =>
        val partitions = task.data.grouped(1024).toVector
        val maxAckRetries = 3

        def retryKey(partitionIdx: Int): String = s"retry-work-$partitionIdx"

        extension (state: MasterState) {
          def spawnWorker(): (MasterState, WorkerRef) = {
            val spawned = context.spawn(
              Behaviors.supervise(Worker(enableCrashForTest)).onFailure(SupervisorStrategy.stop.withLoggingEnabled(true)),
              s"worker-${state.nextWorkerNumber}",
              DispatcherSelector.fromConfig("cpu-dispatcher"))
            context.watchWith(spawned, WorkerDied(spawned))

            (state.copy(
              workers = state.workers + (spawned -> WorkerState.Available),
              nextWorkerNumber = state.nextWorkerNumber + 1
            ), spawned)
          }

          def spawnWorkers(n: Int): MasterState = {
            (0 until n).foldLeft(state)((state, _) => state.spawnWorker()._1)
          }

          def sendNextWorkToAllWorkers(): MasterState = {
            state.workers.keys.foldLeft(state)((state, w) => state.sendNextWork(w))
          }

          def removeWorker(worker: WorkerRef): MasterState = {
            state.workers.get(worker) match {
              case Some(workerState) =>
                val newState = workerState match {
                  case Assigned(partitionIdx) =>
                    timers.cancel(retryKey(partitionIdx))
                    state.copy(
                      partitionStates = state.partitionStates.updated(partitionIdx, PartitionState.Waiting),
                      waitingPartitions = state.waitingPartitions + partitionIdx
                    )
                  case Available => state
                }
                newState.copy(
                  workers = state.workers - worker,
                )
              case None => state
            }
          }

          def replaceWorker(worker: WorkerRef): (MasterState, WorkerRef) = {
            context.stop(worker)
            state
              .removeWorker(worker)
              .spawnWorker()
          }

          def sendWork(partitionIdx: Int, worker: WorkerRef): MasterState = {
            val workerIsAvailable = state.workers.get(worker).contains(WorkerState.Available)
            val partitionIsWaiting = state.partitionStates(partitionIdx) == PartitionState.Waiting

            if (!workerIsAvailable || !partitionIsWaiting) {
              state
            } else {
              println(s"[MASTER] Dispatched partition $partitionIdx to ${worker.path.name}")
              worker ! WorkBatch(partitionIdx, partitions(partitionIdx), task.stage, context.self)
              timers.startTimerWithFixedDelay(
                retryKey(partitionIdx),
                RetrySendWork(partitionIdx),
                5.seconds
              )

              state.copy(
                partitionStates = state.partitionStates.updated(partitionIdx,
                  PartitionState.Dispatched(worker, Instant.now(), retryCount = 0)),
                waitingPartitions = state.waitingPartitions - partitionIdx,
                workers = state.workers.updated(worker, WorkerState.Assigned(partitionIdx))
              )
            }
          }

          def sendNextWork(worker: WorkerRef): MasterState =
            state.waitingPartitions.headOption match {
              case Some(waitingIdx) => state.sendWork(waitingIdx, worker)
              case None => state
            }
        }

        def active(oldState: MasterState): Behavior[MasterMessage] =
          Behaviors.receiveMessage {
            case WorkAccepted(idx, worker) =>
              println(s"[MASTER] Confirmation received: ${worker.path.name} processing partition $idx")
              val newState = oldState.partitionStates(idx) match {
                case PartitionState.Dispatched(w, _, _) if w == worker =>
                  timers.cancel(retryKey(idx))
                  oldState.copy(
                    partitionStates = oldState.partitionStates.updated(idx, Processing(worker, Instant.now())),
                    waitingPartitions = oldState.waitingPartitions - idx,
                    workers = oldState.workers.updated(worker, WorkerState.Assigned(idx))
                  )
                case _ => oldState
              }

              active(newState)
            case WorkDone(idx, res, worker) =>
              println(s"[MASTER] Partition $idx completed successfully by ${worker.path.name} (${oldState.waitingPartitions.size} remaining)")
              val maybeNewState = oldState.partitionStates(idx) match {
                case PartitionState.Processing(w, _) if w == worker =>
                  timers.cancel(retryKey(idx))
                  Some(oldState.copy(
                    partitionStates = oldState.partitionStates.updated(idx, PartitionState.Done(res)),
                    waitingPartitions = oldState.waitingPartitions - idx,
                    workers = oldState.workers.updated(worker, WorkerState.Available),
                    partitionsDone = oldState.partitionsDone + 1
                  ))
                case PartitionState.Dispatched(w, _, _) if w == worker =>
                  timers.cancel(retryKey(idx))
                  Some(oldState.copy(
                    partitionStates = oldState.partitionStates.updated(idx, PartitionState.Done(res)),
                    waitingPartitions = oldState.waitingPartitions - idx,
                    workers = oldState.workers.updated(worker, WorkerState.Available),
                    partitionsDone = oldState.partitionsDone + 1
                  ))
                case _ =>
                  None
              }

              maybeNewState match {
                case Some(newState) if newState.partitionsDone == partitions.size =>
                  // All work done: Reconstitute in order
                  val allParts = newState.partitionStates.asInstanceOf[Vector[PartitionState.Done]]
                  val reassembledPartitions = allParts.flatMap(_.result).asInstanceOf[Vector[O]]
                  println(s"--- All partitions have been received! ---")
                  val finalData = task.stage.reducer(reassembledPartitions)
                  println(finalData)
                  Behaviors.stopped
                case Some(newState) =>
                  active(newState.sendNextWork(worker))
                case None =>
                  active(oldState)
              }
            case WorkerDied(worker) =>
              println(s"[MASTER] ERROR: ${worker.path.name} died! Re-evaluating assigned partitions...")
              val interruptedPartitions = oldState.partitionStates.zipWithIndex.collect {
                case (PartitionState.Dispatched(w, _, _), i) if w == worker => i
                case (Processing(w, _), i) if w == worker => i
              }

              if (!oldState.workers.contains(worker)) {
                active(oldState)
              } else {
                active(oldState.replaceWorker(worker)._1.sendNextWorkToAllWorkers())
              }
            case RetrySendWork(idx) =>
              val newState = oldState.partitionStates(idx) match {
                case PartitionState.Dispatched(worker, _, retryCount) =>
                  val nextRetryCount = retryCount + 1

                  if (nextRetryCount <= maxAckRetries) {
                    worker ! WorkBatch(idx, partitions(idx), task.stage, context.self)
                    oldState.copy(
                      partitionStates = oldState.partitionStates.updated(
                        idx,
                        PartitionState.Dispatched(worker, Instant.now(), nextRetryCount)
                      )
                    )
                  } else {
                    timers.cancel(retryKey(idx))
                    val (newState, _) = oldState.copy(
                      partitionStates = oldState.partitionStates.updated(idx, Waiting),
                      waitingPartitions = oldState.waitingPartitions + idx
                    ).replaceWorker(worker)
                    newState.sendNextWorkToAllWorkers()
                  }
                case _ =>
                  oldState
              }
              active(newState)
          }

        val state = MasterState(
          partitionStates = partitions.map(_ => PartitionState.Waiting),
          waitingPartitions = partitions.indices.toSet,
          workers = Map.empty,
          nextWorkerNumber = 1,
          partitionsDone = 0
        )
          .spawnWorkers(8)
          .sendNextWorkToAllWorkers()

        active(state)
      }
    }
}

@main
def main(args: String*): Unit = {
  val mode = args.headOption.getOrElse("run")
  val enableCrashForTest = mode == "test1"

  val rawData = (1L to 100_000_000L).toVector // Example data

  def map(vector: Vector[Long]): Vector[(Long, Int)] = {
    vector
      .map(x => x * x)
      .groupBy(_ % 10)
      .map((k, v) => (k, v.size))
      .toVector
  }

  def reduce(vector: Vector[(Long, Int)]): Map[Long, Int] = {
    vector.groupMapReduce(_._1)(_._2)(_ + _)
  }

  val system = ActorSystem(
    Master(Task(rawData, Stage(map, reduce)), enableCrashForTest),
    "ProcessingSystem"
  )
}