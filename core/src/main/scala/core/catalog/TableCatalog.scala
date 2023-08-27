package core.catalog

case class TableCatalog(
  columns: Seq[(String, Class[_])],
  metadata: Map[String, String] = Map.empty
) {
  val columnName: Seq[String]          = columns.map(_._1)
  val columnMap: Map[String, Class[_]] = columns.toMap

  def fieldIndex(name: String): Int = columnName.indexOf(name)
}
