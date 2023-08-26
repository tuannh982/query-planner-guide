package core.planner.volcano.physicalplan.builder

import core.execution.ProjectOperator
import core.planner.volcano.cost.Cost
import core.planner.volcano.physicalplan.{Estimations, PhysicalPlan, PhysicalPlanBuilder, Project}

class ProjectionImpl extends PhysicalPlanBuilder {

  override def build(children: Seq[PhysicalPlan]): PhysicalPlan = {
    val child = children.head
    val selfCost = Cost(
      estimatedCpuCost = 0,
      estimatedMemoryCost = 0,
      estimatedTimeCost = 0
    ) // assuming the cost of projection is 0
    val cost = Cost(
      estimatedCpuCost = selfCost.estimatedCpuCost + child.cost().estimatedCpuCost,
      estimatedMemoryCost = selfCost.estimatedMemoryCost + child.cost().estimatedMemoryCost,
      estimatedTimeCost = selfCost.estimatedTimeCost + child.cost().estimatedTimeCost
    )
    val estimations = Estimations(
      estimatedLoopIterations = child.estimations().estimatedLoopIterations,
      estimatedRowSize = child.estimations().estimatedRowSize // just guess
    )
    Project(
      operator = ProjectOperator(child.operator()),
      child = child,
      cost = cost,
      estimations = estimations
    )
  }
}
