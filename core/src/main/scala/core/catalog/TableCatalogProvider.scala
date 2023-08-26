package core.catalog

trait TableCatalogProvider {
  def catalog(table: String): TableCatalog
}
