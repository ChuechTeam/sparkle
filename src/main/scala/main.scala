import akka.actor.typed.{ActorRef, Behavior, ActorSystem}
import akka.actor.typed.scaladsl.Behaviors
import scala.collection.immutable.TreeMap

// Marker trait for Kryo serialization
//trait SerializableMessage

// Actions ADT
sealed trait Action[T]
case class FilterAction[T](f: T => Boolean) extends Action[T]
case class MapAction[T](f: T => T) extends Action[T]

// Messages
case class WorkBatch[T](index: Int, data: Vector[T], actions: List[Action[T]], replyTo: ActorRef[WorkerResponse[T]])
sealed trait WorkerResponse[T]
case class WorkDone[T](index: Int, result: Vector[T], worker: ActorRef[WorkBatch[T]]) extends WorkerResponse[T]

object Worker {
  def apply[T](): Behavior[WorkBatch[T]] = Behaviors.receiveMessage { msg =>
    // Apply the sequence of operations to the batch
    val processed = msg.actions.foldLeft(msg.data) { (acc, action) =>
      action match {
        case FilterAction(f) => acc.filter(f)
        case MapAction(f)    => acc.map(f)
      }
    }
    msg.replyTo ! WorkDone(msg.index, processed, msg.replyTo.unsafeUpcast)
    Behaviors.same
  }
}

object Master {
  def apply[T](data: Vector[T], actions: List[Action[T]]): Behavior[WorkerResponse[T]] =
    Behaviors.setup { context =>
      // Now we have
      // [list 1024, list 1024, ..., list 1024]
      val partitions = data.grouped(1024).toVector
      val partitionCount = partitions.size

      // Spawn 8 workers
      val workers = (1 to 8).map(i => context.spawn(Worker[T](), s"worker-$i"))

      def active(nextIdx: Int, results: TreeMap[Int, Vector[T]]): Behavior[WorkerResponse[T]] =
        Behaviors.receiveMessage {
          case WorkDone(idx, res, worker) =>
            val newResults = results + (idx -> res)

            if (newResults.size == partitionCount) {
              // All work done: Reconstitute in order
              val finalData = newResults.values.flatten
              println(s"--- Processing Complete ---")
              finalData.foreach(println)
              Behaviors.stopped
            } else {
              // Assign next available partition to the worker that just finished
              if (nextIdx < partitionCount) {
                worker ! WorkBatch(nextIdx, partitions(nextIdx), actions, context.self)
                active(nextIdx + 1, newResults)
              } else {
                active(nextIdx, newResults)
              }
            }
        }

      // Initial assignment: give each worker one partition to start
      val firstBatchSize = math.min(8, partitionCount)
      for (i <- 0 until firstBatchSize) {
        workers(i) ! WorkBatch(i, partitions(i), actions, context.self)
      }

      active(firstBatchSize, TreeMap.empty)
    }
}

@main
def main(): Unit = {
  val rawData = (1 to 5000).toVector // Example data
  val actions = List(
    FilterAction[Int](_ < 100),    // Logic: Keep numbers < 100
    MapAction[Int](_ * 10)         // Logic: Multiply by 10
  )

  val system = ActorSystem(Master(rawData, actions), "ProcessingSystem")
}