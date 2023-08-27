package core.ctx

trait Connection {
  def fetchNextRow(table: String): Seq[Any]
}
