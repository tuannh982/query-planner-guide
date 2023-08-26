package core.planner.volcano.rules.implement

import core.planner.volcano.VolcanoPlannerContext
import core.planner.volcano.logicalplan.{Join, Scan}
import core.planner.volcano.physicalplan.PhysicalPlanBuilder
import core.planner.volcano.physicalplan.builder.{HashJoinImpl, MergeJoinImpl, NormalScanImpl}

object Join {

  def apply(node: Join)(implicit ctx: VolcanoPlannerContext): Seq[PhysicalPlanBuilder] = {
    Seq(
      new HashJoinImpl,
      new MergeJoinImpl
    )
  }
}
