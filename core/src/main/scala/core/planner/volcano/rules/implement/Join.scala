package core.planner.volcano.rules.implement

import core.planner.volcano.{logicalplan, VolcanoPlannerContext}
import core.planner.volcano.physicalplan.PhysicalPlanBuilder
import core.planner.volcano.physicalplan.builder.{HashJoinImpl, MergeJoinImpl}

object Join {

  def apply(node: logicalplan.Join)(implicit ctx: VolcanoPlannerContext): Seq[PhysicalPlanBuilder] = {
    Seq(
      new HashJoinImpl,
      new MergeJoinImpl
    )
  }
}
