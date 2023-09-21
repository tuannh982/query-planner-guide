package core.ctx

trait Connection {
  def fetchNextRow(table: String, projection: Seq[String]): Option[Seq[Any]]
}
