import PartitionState.*
import WorkerState.{Assigned, Available}
import ch.qos.logback.classic.{Level, LoggerContext}
import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.slf4j.{Logger, LoggerFactory}

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, DurationInt}
import scala.util.Random

case class Task[I, O, F](data: Vector[I], stage: Stage[I, O, F])

// Accidentally made map reduce
case class Stage[I, O, F](mapper: Vector[I] => Vector[O], reducer: Vector[O] => F)

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

case class WorkDoneAcknowledged(index: Int) extends WorkerMessage

case class WorkerDied(worker: WorkerRef) extends MasterMessage

case class RetrySendWork(index: Int) extends MasterMessage

case class RetryWorkDone(index: Int) extends WorkerMessage

type WorkerRef = ActorRef[WorkerMessage]
type MasterRef = ActorRef[MasterMessage]

val rng = Random()

class FaultState {
  private val ackExhaustReceiveCounts = ConcurrentHashMap[Int, Int]()
  private val crashedAfterAckPartitions = ConcurrentHashMap.newKeySet[Int]()
  private val missedWorkDonePartitions = ConcurrentHashMap.newKeySet[Int]()

  def shouldMissAckUntilExhaustion(partitionIdx: Int, maxReceivesBeforeExhaustion: Int): Boolean = {
    val receiveCount = ackExhaustReceiveCounts.merge(partitionIdx, 1, Integer.sum)
    receiveCount <= maxReceivesBeforeExhaustion
  }

  def shouldCrashAfterAck(partitionIdx: Int): Boolean =
    crashedAfterAckPartitions.add(partitionIdx)

  def shouldMissWorkDone(partitionIdx: Int): Boolean =
    missedWorkDonePartitions.add(partitionIdx)
}

object Worker {
  private val log: Logger = LoggerFactory.getLogger(Worker.getClass)

  def apply(options: Options): Behavior[WorkerMessage] = Behaviors.withTimers { timers =>
    var pendingWorkDone: Option[(Int, Vector[?], MasterRef)] = None

    Behaviors.receive { (ctx, msg) =>
      msg match {
        case WorkBatch(index, data, stage, master) =>
          ctx.log.debug(s"${ctx.self.path.name} Received partition $index")
          if (options.autoCrash && rng.nextFloat() <= 0.5) {
            ctx.log.warn(s"${ctx.self.path.name} CRASHING INTENTIONALLY !")
            throw RuntimeException(s"[SIMULATED] - Crash on ${ctx.self.path.name}")
          }
          if (options.autoMessageMiss && rng.nextFloat() < 0.5) {
            ctx.log.warn("INTENTIONALLY MISSING A MESSAGE!")
            Behaviors.same
          } else if (options.autoAckExhaust && index == 0 && options.faultState.shouldMissAckUntilExhaustion(index, 4)) {
            ctx.log.warn(s"[SIMULATED-ACK-EXHAUST] ${ctx.self.path.name} intentionally stays silent for partition $index")
            Behaviors.same
          } else {
            master ! WorkAccepted(index, ctx.self)
            if (options.autoCrashAfterAck && index == 1 && options.faultState.shouldCrashAfterAck(index)) {
              ctx.log.warn(s"${ctx.self.path.name} CRASHING INTENTIONALLY AFTER ACK !")
              throw RuntimeException(s"[SIMULATED] - Crash after ACK on ${ctx.self.path.name}")
            }
            val processed = stage.mapper(data)
            ctx.log.debug(s"${ctx.self.path.name} Finished partition $index")
            pendingWorkDone = Some((index, processed, master))
            master ! WorkDone(index, processed, ctx.self)
            timers.startTimerWithFixedDelay(s"retry-work-done-$index", RetryWorkDone(index), 1.second)
            Behaviors.same
          }
        case WorkDoneAcknowledged(index) =>
          ctx.log.debug(s"${ctx.self.path.name} WorkDone for partition $index acknowledged")
          timers.cancel(s"retry-work-done-$index")
          pendingWorkDone = None
          Behaviors.same
        case RetryWorkDone(index) =>
          pendingWorkDone match {
            case Some((idx, result, master)) if idx == index =>
              ctx.log.info(s"[RETRY-WORK-DONE] ${ctx.self.path.name} resends result for partition $index")
              master ! WorkDone(index, result, ctx.self)
              Behaviors.same
            case _ => Behaviors.same
          }
      }
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
  def apply[I, O, F](task: Task[I, O, F], options: Options): Behavior[MasterMessage] =
    Behaviors.setup { context =>
      Behaviors.withTimers { timers =>
        val partitions = task.data.grouped(1024).toVector
        val maxAckRetries = 3

        def retryKey(partitionIdx: Int): String = s"retry-work-$partitionIdx"

        extension (state: MasterState) {
          def spawnWorker(): (MasterState, WorkerRef) = {
            val spawned = context.spawn(
              Behaviors.supervise(Worker(options)).onFailure(SupervisorStrategy.stop.withLoggingEnabled(true)),
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
                    context.log.info(
                      s"[REQUEUE] partition $partitionIdx (worker ${worker.path.name} removed while Assigned)"
                    )
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
              context.log.debug(s"Dispatched partition $partitionIdx to ${worker.path.name}")
              worker ! WorkBatch(partitionIdx, partitions(partitionIdx), task.stage, context.self)
              timers.startTimerWithFixedDelay(
                retryKey(partitionIdx),
                RetrySendWork(partitionIdx),
                500.millis
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
              context.log.debug(s"Confirmation received: ${worker.path.name} processing partition $idx")
              val newState = oldState.partitionStates(idx) match {
                case PartitionState.Dispatched(w, _, _) if w == worker =>
                  timers.cancel(retryKey(idx))
                  oldState.copy(
                    partitionStates = oldState.partitionStates.updated(idx, Processing(worker, Instant.now())),
                    waitingPartitions = oldState.waitingPartitions - idx,
                    workers = oldState.workers.updated(worker, WorkerState.Assigned(idx))
                  )
                case currentState =>
                  context.log.info(
                    s"[IGNORED-ACK] partition $idx from ${worker.path.name} ignored because current state is $currentState"
                  )
                  oldState
              }

              active(newState)
            case WorkDone(idx, res, worker) =>
              if (options.autoWorkDoneMiss && idx == 2 && options.faultState.shouldMissWorkDone(idx)) {
                context.log.warn(
                  s"[SIMULATED-WORKDONE-MISS] master intentionally ignores first WorkDone for partition $idx from ${worker.path.name}"
                )
                active(oldState)
              } else {
                def acknowledgeWorkDone(): Unit = {
                  worker ! WorkDoneAcknowledged(idx)
                }

                val maybeNewState = oldState.partitionStates(idx) match {
                  case PartitionState.Processing(w, _) if w == worker =>
                    timers.cancel(retryKey(idx))
                    context.log.info(s"Partition $idx completed successfully by ${worker.path.name} (${oldState.waitingPartitions.size} remaining)")
                    acknowledgeWorkDone()
                    Some(oldState.copy(
                      partitionStates = oldState.partitionStates.updated(idx, PartitionState.Done(res)),
                      waitingPartitions = oldState.waitingPartitions - idx,
                      workers = oldState.workers.updated(worker, WorkerState.Available),
                      partitionsDone = oldState.partitionsDone + 1
                    ))
                  case PartitionState.Dispatched(w, _, _) if w == worker =>
                    timers.cancel(retryKey(idx))
                    context.log.info(s"Partition $idx completed successfully by ${worker.path.name} (${oldState.waitingPartitions.size} remaining)")
                    acknowledgeWorkDone()
                    Some(oldState.copy(
                      partitionStates = oldState.partitionStates.updated(idx, PartitionState.Done(res)),
                      waitingPartitions = oldState.waitingPartitions - idx,
                      workers = oldState.workers.updated(worker, WorkerState.Available),
                      partitionsDone = oldState.partitionsDone + 1
                    ))
                  case PartitionState.Done(_) =>
                    context.log.info(
                      s"[IGNORED-DONE] partition $idx from ${worker.path.name} ignored because it is already Done"
                    )
                    acknowledgeWorkDone()
                    None
                  case currentState =>
                    context.log.info(
                      s"[IGNORED-DONE] partition $idx from ${worker.path.name} ignored because current state is $currentState"
                    )
                    None
                }

                maybeNewState match {
                  case Some(newState) if newState.partitionsDone == partitions.size =>
                    // All work done: Reconstitute in order
                    val allParts = newState.partitionStates.asInstanceOf[Vector[PartitionState.Done]]
                    val reassembledPartitions = allParts.flatMap(_.result).asInstanceOf[Vector[O]]
                    context.log.info(s"--- All partitions have been received! ---")
                    val finalData = task.stage.reducer(reassembledPartitions)
                    context.log.info("Data: \n {}", finalData)
                    Behaviors.stopped
                  case Some(newState) =>
                    active(newState.sendNextWork(worker))
                  case None =>
                    active(oldState)
                }
              }
            case WorkerDied(worker) =>
              context.log.warn(s"ERROR: ${worker.path.name} died! Re-evaluating assigned partitions...")
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
                    context.log.info(
                      s"[RETRY-ACK] attempt $nextRetryCount/$maxAckRetries: partition $idx still Dispatched, resending WorkBatch to ${worker.path.name}"
                    )
                    worker ! WorkBatch(idx, partitions(idx), task.stage, context.self)
                    oldState.copy(
                      partitionStates = oldState.partitionStates.updated(
                        idx,
                        PartitionState.Dispatched(worker, Instant.now(), nextRetryCount)
                      )
                    )
                  } else {
                    timers.cancel(retryKey(idx))
                    context.log.info(
                      s"[RETRY-ACK-EXHAUSTED] partition $idx on ${worker.path.name}: re-queue, replace worker, redispatch to available workers"
                    )
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

case class Options(
                    autoCrash: Boolean,
                    autoMessageMiss: Boolean,
                    quiet: Boolean,
                    autoCrashAfterAck: Boolean,
                    autoAckExhaust: Boolean,
                    autoWorkDoneMiss: Boolean,
                    faultState: FaultState
                  )

@main
def main(args: String*): Unit = {
  val options = Options(
    args.contains("auto-crash"),
    args.contains("auto-message-miss"),
    args.contains("auto-crash-after-ack"),
    args.contains("auto-ack-exhaust"),
    args.contains("auto-workdone-miss"),
    args.contains("quiet"),
    FaultState()
  )

  // configure logging to be more or less chatty
  val loggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
  val pekkoLogger = loggerContext.getLogger("org.apache")
  pekkoLogger.setLevel(Level.INFO)

  val rootLogger = loggerContext.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
  rootLogger.setLevel(if (options.quiet) {
    Level.INFO
  } else {
    Level.DEBUG
  })

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
    Master(Task(rawData, Stage(map, reduce)), options),
    "ProcessingSystem"
  )

  Await.result(system.whenTerminated, Duration.Inf)
}
