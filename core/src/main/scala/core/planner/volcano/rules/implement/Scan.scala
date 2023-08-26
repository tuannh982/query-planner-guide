package core.planner.volcano.rules.implement

import core.planner.volcano.VolcanoPlannerContext
import core.planner.volcano.logicalplan.Scan
import core.planner.volcano.physicalplan.builder.NormalScanImpl
import core.planner.volcano.physicalplan.{Estimations, PhysicalPlan, PhysicalPlanBuilder}

object Scan {

  def apply(node: Scan)(implicit ctx: VolcanoPlannerContext): Seq[PhysicalPlanBuilder] = {
    val tableName  = node.table.id
    val tableStats = ctx.statsProvider.tableStats(node.table.id)
    val projection = node.projection
    Seq(
      new NormalScanImpl(tableName, tableStats, projection)
    )
  }
}
