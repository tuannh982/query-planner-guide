package utils.generator

trait Generator[T] {
  def generate(): T
}
