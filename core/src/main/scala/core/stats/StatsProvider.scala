package core.stats

trait StatsProvider {
  def tableStats(table: String): TableStats
}
