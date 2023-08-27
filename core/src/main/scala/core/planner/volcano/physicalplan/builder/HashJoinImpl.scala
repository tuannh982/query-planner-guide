package core.planner.volcano.physicalplan.builder

import core.execution.HashJoinOperator
import core.planner.volcano.cost.Cost
import core.planner.volcano.physicalplan.{Estimations, Join, PhysicalPlan, PhysicalPlanBuilder}

class HashJoinImpl extends PhysicalPlanBuilder {

  private def viewSize(plan: PhysicalPlan): Long = {
    plan.estimations().estimatedLoopIterations * plan.estimations().estimatedRowSize
  }

  //noinspection ZeroIndexToHead,DuplicatedCode
  override def build(children: Seq[PhysicalPlan]): Option[PhysicalPlan] = {
    // reorder the child nodes, the left child is the child with smaller view size (smaller than the right child if we're store all of them in memory)
    val (leftChild, rightChild) = if (viewSize(children(0)) < viewSize(children(1))) {
      (children(0), children(1))
    } else {
      (children(1), children(0))
    }
    val estimatedLoopIterations = Math.max(
      leftChild.estimations().estimatedLoopIterations,
      rightChild.estimations().estimatedLoopIterations
    ) // just guessing the value
    val estimatedOutRowSize = leftChild.estimations().estimatedRowSize + rightChild.estimations().estimatedRowSize
    val selfCost = Cost(
      estimatedCpuCost = leftChild.estimations().estimatedLoopIterations, // cost to hash all record from the smaller view
      estimatedMemoryCost = viewSize(leftChild),                          // hash the smaller view, we need to hold the hash table in memory
      estimatedTimeCost = rightChild.estimations().estimatedLoopIterations
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
        operator = HashJoinOperator(
          leftChild.operator(),
          rightChild.operator(),
          Seq("id"), // hard-coded join field
          Seq("id")  // hard-coded join field
        ),
        leftChild = leftChild,
        rightChild = rightChild,
        cost = cost,
        estimations = estimations,
        traits = Set.empty // don't inherit trait from children since we're hash join
      )
    )
  }
}
