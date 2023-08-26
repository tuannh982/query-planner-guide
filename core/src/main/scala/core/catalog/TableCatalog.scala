package core.catalog

case class TableCatalog(
  columns: Map[String, Class[_]]
)
