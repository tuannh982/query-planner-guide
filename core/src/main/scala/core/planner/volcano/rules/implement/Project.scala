package core.planner.volcano.rules.implement

import core.planner.volcano.VolcanoPlannerContext
import core.planner.volcano.logicalplan.{Project, Scan}
import core.planner.volcano.physicalplan.PhysicalPlanBuilder
import core.planner.volcano.physicalplan.builder.ProjectionImpl

object Project {

  def apply(node: Project)(implicit ctx: VolcanoPlannerContext): Seq[PhysicalPlanBuilder] = {
    Seq(
      new ProjectionImpl
    )
  }
}
