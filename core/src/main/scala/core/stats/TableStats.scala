package core.stats

case class TableStats(
  estimatedRowCount: Long,
  avgColumnSize: Map[String, Long]
) {
  val avgRowSize: Long         = avgColumnSize.values.sum
  val estimatedTableSize: Long = estimatedRowCount * avgRowSize
}
