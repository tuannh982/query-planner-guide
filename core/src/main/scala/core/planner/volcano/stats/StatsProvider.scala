package core.planner.volcano.stats

trait StatsProvider {
  def tableStats(table: String): TableStats
}
