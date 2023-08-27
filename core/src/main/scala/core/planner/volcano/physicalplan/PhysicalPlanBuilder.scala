package core.planner.volcano.physicalplan

trait PhysicalPlanBuilder {
  def build(children: Seq[PhysicalPlan]): Option[PhysicalPlan]
}
