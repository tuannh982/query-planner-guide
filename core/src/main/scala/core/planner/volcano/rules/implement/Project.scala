package core.planner.volcano.rules.implement

import core.planner.volcano.{logicalplan, VolcanoPlannerContext}
import core.planner.volcano.physicalplan.PhysicalPlanBuilder
import core.planner.volcano.physicalplan.builder.ProjectionImpl

object Project {

  def apply(node: logicalplan.Project)(implicit ctx: VolcanoPlannerContext): Seq[PhysicalPlanBuilder] = {
    Seq(
      new ProjectionImpl
    )
  }
}
