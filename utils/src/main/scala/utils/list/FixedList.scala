package utils.list

sealed trait FixedList[T] {
  def values: Seq[T]
}

case class List0[T]() extends FixedList[T] {
  override def values: Seq[T] = Seq.empty
}

case class List1[T, A0 <: T](a0: A0) extends FixedList[T] {
  override def values: Seq[T] = Seq(a0)
}

case class List2[T, A0 <: T, A1 <: T](a0: A0, a1: A1) extends FixedList[T] {
  override def values: Seq[T] = Seq(a0, a1)
}
