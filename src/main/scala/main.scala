import PartitionState.Processing
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior, SupervisorStrategy}

import java.time.Instant
import scala.concurrent.duration.DurationInt
import scala.util.Random
import scala.util.chaining.scalaUtilChainingOps

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

case class WorkBatch(index: Int, data: Vector[?], stage: Stage, master: MasterRef) extends WorkerMessage

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
  def apply(enableCrashForTest: Boolean): Behavior[WorkerMessage] = Behaviors.receive { (ctx, msg) =>
    msg match {
      case WorkBatch(index, data, stage, master) =>
        if (enableCrashForTest && ctx.self.path.name == "worker-1") {
          throw RuntimeException(s"test1: simulated crash on ${ctx.self.path.name}")
        }
        master ! WorkAccepted(index, ctx.self)
        val processed = stage.actions.foldLeft(data) { (acc, action) => action.applyUnsafe(acc) }
        master ! WorkDone(index, processed, ctx.self)
        Behaviors.same
    }
  }
}

enum PartitionState {
  case Waiting
  case Dispatched(who: WorkerRef, since: Instant)
  case Processing(who: WorkerRef, since: Instant)
  case Done(result: Vector[?])
}

case class MasterState(partitionStates: Vector[PartitionState],
                       workers: Set[WorkerRef],
                       nextWorkerNumber: Int,
                       nextPartitionIndex: Int,
                       partitionsDone: Int)

object Master {
  def apply(task: Task, enableCrashForTest: Boolean): Behavior[MasterMessage] =
    Behaviors.setup { context =>
      Behaviors.withTimers { timers =>
        val partitions = task.data.grouped(1024).toVector
        def retryKey(partitionIdx: Int): String = s"retry-work-$partitionIdx"

        extension (state: MasterState) {
          def spawnWorker(): (MasterState, WorkerRef) = {
            val spawned = context.spawn(
              Behaviors.supervise(Worker(enableCrashForTest)).onFailure(SupervisorStrategy.stop.withLoggingEnabled(true)),
              s"worker-${state.nextWorkerNumber}")
            context.watchWith(spawned, WorkerDied(spawned))

            (state.copy(
              workers = state.workers + spawned,
              nextWorkerNumber = state.nextWorkerNumber + 1
            ), spawned)
          }

          def spawnWorkers(n: Int): MasterState = {
            (0 until n).foldLeft(state)((state, _) => state.spawnWorker()._1)
          }

          def sendNextWorkToAllWorkers(): MasterState = {
            state.workers.foldLeft(state)((state, w) => state.sendNextWork(w))
          }

          def removeWorker(worker: WorkerRef): MasterState = {
            state.copy(
              workers = state.workers - worker
            )
          }

          def sendWork(partitionIdx: Int, worker: WorkerRef): MasterState = {
            worker ! WorkBatch(partitionIdx, partitions(partitionIdx), task.stage, context.self)
            timers.startTimerWithFixedDelay(
              retryKey(partitionIdx),
              RetrySendWork(partitionIdx),
              5.seconds
            )

            state.copy(
              partitionStates = state.partitionStates.updated(partitionIdx,
                PartitionState.Dispatched(worker, Instant.now()))
            )
          }

          def sendNextWork(worker: WorkerRef): MasterState = {
            if (state.nextPartitionIndex == partitions.size) {
              state
            }
            else {
              state.sendWork(state.nextPartitionIndex, worker).copy(
                nextPartitionIndex = state.nextPartitionIndex + 1
              )
            }
          }

        }

        def active(oldState: MasterState): Behavior[MasterMessage] =
          Behaviors.receiveMessage {
            case WorkAccepted(idx, worker) =>
              val newState = oldState.partitionStates(idx) match {
                case PartitionState.Dispatched(w, _) if w == worker =>
                  timers.cancel(retryKey(idx))
                  oldState.copy(
                    partitionStates = oldState.partitionStates.updated(idx, Processing(worker, Instant.now()))
                  )
                case _ => oldState
              }

              active(newState)
            case WorkDone(idx, res, worker) =>
              val maybeNewState = oldState.partitionStates(idx) match {
                case PartitionState.Processing(w, _) if w == worker =>
                  timers.cancel(retryKey(idx))
                  Some(oldState.copy(
                    partitionStates = oldState.partitionStates.updated(idx, PartitionState.Done(res)),
                    partitionsDone = oldState.partitionsDone + 1
                  ))
                case PartitionState.Dispatched(w, _) if w == worker =>
                  timers.cancel(retryKey(idx))
                  Some(oldState.copy(
                    partitionStates = oldState.partitionStates.updated(idx, PartitionState.Done(res)),
                    partitionsDone = oldState.partitionsDone + 1
                  ))
                case _ =>
                  None
              }

              maybeNewState match {
                case Some(newState) if newState.partitionsDone == partitions.size =>
                  // All work done: Reconstitute in order
                  val allParts = newState.partitionStates.asInstanceOf[Vector[PartitionState.Done]]
                  val finalData = allParts.flatMap(_.result)
                  println(s"--- Processing Complete ---")
                  finalData.foreach(println)
                  Behaviors.stopped
                case Some(newState) =>
                  active(newState.sendNextWork(worker))
                case None =>
                  active(oldState)
              }
            case WorkerDied(worker) =>
              val interruptedPartitions = oldState.partitionStates.zipWithIndex.collect {
                case (PartitionState.Dispatched(w, _), i) if w == worker => i
                case (Processing(w, _), i) if w == worker => i
              }

              val stateWithoutWorker = oldState.removeWorker(worker)
              val (stateWithReplacement, replacement) = stateWithoutWorker.spawnWorker()
              val reassignedState = interruptedPartitions.foldLeft(stateWithReplacement) { (state, partitionIdx) =>
                state.sendWork(partitionIdx, replacement)
              }

              active(reassignedState.sendNextWork(replacement))
            case RetrySendWork(idx) =>
              val newState = oldState.partitionStates(idx) match {
                case PartitionState.Dispatched(worker, _) =>
                  worker ! WorkBatch(idx, partitions(idx), task.stage, context.self)
                  oldState.copy(
                    partitionStates = oldState.partitionStates.updated(idx, PartitionState.Dispatched(worker, Instant.now()))
                  )
                case _ =>
                  oldState
              }
              active(newState)
          }

        val state = MasterState(
          partitionStates = partitions.map(_ => PartitionState.Waiting),
          workers = Set.empty,
          nextWorkerNumber = 1,
          nextPartitionIndex = 0,
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

  val rawData = (1 to 50000).toVector // Example data
  val actions = List(
    FilterAction[Int](_ < 100), // Logic: Keep numbers < 100
    MapAction[Int, String](x => "Nombre : " + x) // map stuff
  )

  val system = ActorSystem(
    Master(Task(rawData, Stage(actions, Option.empty)), enableCrashForTest),
    "ProcessingSystem"
  )
}