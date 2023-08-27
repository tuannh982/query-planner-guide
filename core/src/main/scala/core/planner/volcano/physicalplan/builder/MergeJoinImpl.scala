package core.planner.volcano.physicalplan.builder

import core.execution.MergeJoinOperator
import core.planner.volcano.cost.Cost
import core.planner.volcano.physicalplan.{Estimations, Join, PhysicalPlan, PhysicalPlanBuilder}

class MergeJoinImpl extends PhysicalPlanBuilder {

  //noinspection ZeroIndexToHead,DuplicatedCode
  override def build(children: Seq[PhysicalPlan]): Option[PhysicalPlan] = {
    val (leftChild, rightChild) = (children(0), children(1))
    if (leftChild.traits().contains("SORTED") && rightChild.traits().contains("SORTED")) {
      val estimatedTotalRowCount =
        leftChild.estimations().estimatedLoopIterations +
          rightChild.estimations().estimatedLoopIterations
      val estimatedLoopIterations = Math.max(
        leftChild.estimations().estimatedLoopIterations,
        rightChild.estimations().estimatedLoopIterations
      ) // just guessing the value
      val estimatedOutRowSize = leftChild.estimations().estimatedRowSize + rightChild.estimations().estimatedRowSize
      val selfCost = Cost(
        estimatedCpuCost = 0,    // no additional cpu cost, just scan from child iterator
        estimatedMemoryCost = 0, // no additional memory cost
        estimatedTimeCost = estimatedTotalRowCount
      )
      val childCosts = Cost(
        estimatedCpuCost = leftChild.cost().estimatedCpuCost + rightChild.cost().estimatedCpuCost,
        estimatedMemoryCost = leftChild.cost().estimatedMemoryCost + rightChild.cost().estimatedMemoryCost,
        estimatedTimeCost = 0
      )
      val estimations = Estimations(
        estimatedLoopIterations = estimatedLoopIterations,
        estimatedRowSize = estimatedOutRowSize
      )
      val cost = Cost(
        estimatedCpuCost = selfCost.estimatedCpuCost + childCosts.estimatedCpuCost,
        estimatedMemoryCost = selfCost.estimatedMemoryCost + childCosts.estimatedMemoryCost,
        estimatedTimeCost = selfCost.estimatedTimeCost + childCosts.estimatedTimeCost
      )
      Some(
        Join(
          operator = MergeJoinOperator(
            leftChild.operator(),
            rightChild.operator(),
            Seq("id"), // hard-coded join field
            Seq("id")  // hard-coded join field
          ),
          leftChild = leftChild,
          rightChild = rightChild,
          cost = cost,
          estimations = estimations,
          traits = leftChild.traits() ++ rightChild.traits()
        )
      )
    } else {
      None
    }
  }
}
