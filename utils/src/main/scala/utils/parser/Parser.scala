package utils.parser

trait Parser[T] {
  def parse(in: String): Either[Throwable, T]
}
