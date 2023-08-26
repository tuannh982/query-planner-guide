package core.planner.volcano.rules.implement

import core.planner.volcano.physicalplan.PhysicalPlanBuilder
import core.planner.volcano.physicalplan.builder.NormalScanImpl
import core.planner.volcano.{logicalplan, VolcanoPlannerContext}

object Scan {

  def apply(node: logicalplan.Scan)(implicit ctx: VolcanoPlannerContext): Seq[PhysicalPlanBuilder] = {
    val tableName  = node.table.id
    val tableStats = ctx.statsProvider.tableStats(node.table.id)
    val projection = node.projection
    Seq(
      new NormalScanImpl(tableName, tableStats, projection)
    )
  }
}
