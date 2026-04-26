sealed trait Action[T] {
  def apply(vector: Vector[T]): Vector[?]

  def applyUnsafe(vector: Vector[?]): Vector[?] = this (vector.asInstanceOf[Vector[T]])
}

case class FilterAction[T](f: T => Boolean) extends Action[T] {
  def apply(vector: Vector[T]): Vector[?] = vector.filter(f)
}

case class MapAction[T, U](f: T => U) extends Action[T] {
  def apply(vector: Vector[T]): Vector[?] = vector.map(f)
}

case class FlatMapAction[T, U](f: T => IterableOnce[U]) extends Action[T] {
  def apply(vector: Vector[T]): Vector[?] = vector.flatMap(f)
}

case class Task(data: Vector[?], stage: Stage)

case class Stage(actions: List[Action[?]], reducer: Option[Reducer[?, ?, ?]])

// todo: LATER!
case class Reducer[I, K, O](splitter: I => K, merger: (K, Vector[I]) => Vector[O])