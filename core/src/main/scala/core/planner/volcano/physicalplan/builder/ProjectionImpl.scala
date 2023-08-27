package core.planner.volcano.physicalplan.builder

import core.execution.ProjectOperator
import core.planner.volcano.cost.Cost
import core.planner.volcano.physicalplan.{Estimations, PhysicalPlan, PhysicalPlanBuilder, Project}
import core.ql

class ProjectionImpl(projection: Seq[ql.FieldID]) extends PhysicalPlanBuilder {

  override def build(children: Seq[PhysicalPlan]): Option[PhysicalPlan] = {
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
      estimatedRowSize = child.estimations().estimatedRowSize // just guessing the value
    )
    Some(
      Project(
        operator = ProjectOperator(projection, child.operator()),
        child = child,
        cost = cost,
        estimations = estimations,
        traits = child.traits()
      )
    )
  }
}
