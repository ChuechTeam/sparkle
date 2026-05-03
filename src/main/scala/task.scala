case class Task[I, O, F](data: Vector[I], stage: Stage[I, O, F])

// Accidentally made map reduce
case class Stage[I, O, F](mapper: Vector[I] => Vector[O], reducer: Vector[O] => F)