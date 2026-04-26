import PartitionState.Processing
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior, SupervisorStrategy}

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
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

case class WorkDone(index: Int, result: Vector[?], worker: WorkerRef) extends MasterMessage

object KillSlackers extends MasterMessage

// not sure
//case class WorkFailed(errorMessage: String, worker: ActorRef[WorkBatch[?]])

type WorkerRef = ActorRef[WorkerMessage]
type MasterRef = ActorRef[MasterMessage]

val rng = Random()

object Worker {
  def apply(): Behavior[WorkerMessage] = Behaviors.receive { (ctx, msg) =>
    msg match {
      case WorkBatch(index, data, stage, master) =>
        val processed = stage.actions.foldLeft(data) { (acc, action) => action.applyUnsafe(acc) }
        master ! WorkDone(index, processed, ctx.self)
        Behaviors.same
    }
  }
}

enum PartitionState {
  case Waiting
  case Processing(who: WorkerRef, since: Instant)
  case Done(result: Vector[?])
}

case class MasterState(partitionStates: Vector[PartitionState],
                       workers: Set[WorkerRef],
                       nextWorkerNumber: Int,
                       nextPartitionIndex: Int,
                       partitionsDone: Int)

object Master {
  def apply(task: Task): Behavior[MasterMessage] =
    Behaviors.setup { context =>
      Behaviors.withTimers { timers =>
        val partitions = task.data.grouped(1024).toVector

        extension (state: MasterState) {
          def spawnWorker(): (MasterState, WorkerRef) = {
            val spawned = context.spawn(
              Behaviors.supervise(Worker()).onFailure(SupervisorStrategy.stop.withLoggingEnabled(true)),
              s"worker-${state.nextWorkerNumber}")

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
            context.stop(worker)

            state.copy(
              workers = state.workers - worker
            )
          }

          def sendWork(partitionIdx: Int, worker: WorkerRef): MasterState = {
            worker ! WorkBatch(partitionIdx, partitions(partitionIdx), task.stage, context.self)

            state.copy(
              partitionStates = state.partitionStates.updated(partitionIdx,
                Processing(worker, Instant.now()))
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

          def replaceSlackerAndDoWork(slackerPartitionIdx: Int): MasterState = {
            state.partitionStates(slackerPartitionIdx) match {
              case Processing(slacker, _) =>
                val (newState, worker) = state
                  .removeWorker(slacker)
                  .spawnWorker()

                newState.sendWork(slackerPartitionIdx, worker)
              case _ => state
            }
          }
        }

        def active(oldState: MasterState): Behavior[MasterMessage] =
          Behaviors.receiveMessage {
            case WorkDone(idx, res, worker) =>
              val newState = oldState.copy(
                partitionStates = oldState.partitionStates.updated(idx, PartitionState.Done(res)),
                partitionsDone = oldState.partitionsDone + 1
              )

              if (newState.partitionsDone == partitions.size) {
                // All work done: Reconstitute in order
                val allParts = newState.partitionStates.asInstanceOf[Vector[PartitionState.Done]]
                val finalData = allParts.flatMap(_.result)
                println(s"--- Processing Complete ---")
                finalData.foreach(println)
                Behaviors.stopped
              } else {
                active(newState.sendNextWork(worker))
              }
            case KillSlackers =>
              val now = Instant.now()
              val slackers = oldState.partitionStates.zipWithIndex.collect {
                case (Processing(_, since), i) if now.isAfter(since.plusSeconds(30)) => i
              }

              if (slackers.nonEmpty) {
                println("Slackers found, let's destroy them! " + slackers)
              }

              val newState = slackers.foldLeft(oldState)((state, slacker) => state.replaceSlackerAndDoWork(slacker))
              active(newState)
          }

        timers.startTimerAtFixedRate(KillSlackers, FiniteDuration(1, TimeUnit.SECONDS))

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
def main(): Unit = {
  val rawData = (1 to 50000).toVector // Example data
  val actions = List(
    FilterAction[Int](_ < 100), // Logic: Keep numbers < 100
    MapAction[Int, String](x => "Nombre : " + x) // map stuff
  )

  val system = ActorSystem(Master(Task(rawData, Stage(actions, Option.empty))), "ProcessingSystem")
}