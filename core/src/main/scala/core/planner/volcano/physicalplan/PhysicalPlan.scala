package core.planner.volcano.physicalplan

import core.execution.Operator
import core.planner.volcano.cost.Cost

sealed trait PhysicalPlan {
  def operator(): Operator
  def children(): Seq[PhysicalPlan]
  def cost(): Cost
  def estimations(): Estimations
}

case class Scan(operator: Operator, cost: Cost, estimations: Estimations) extends PhysicalPlan {
  override def children(): Seq[PhysicalPlan] = Seq.empty // scan do not receive any child
}

case class Project(operator: Operator, child: PhysicalPlan, cost: Cost, estimations: Estimations) extends PhysicalPlan {
  override def children(): Seq[PhysicalPlan] = Seq(child)
}

case class Join(
  operator: Operator,
  leftChild: PhysicalPlan,
  rightChild: PhysicalPlan,
  cost: Cost,
  estimations: Estimations
) extends PhysicalPlan {
  override def children(): Seq[PhysicalPlan] = Seq(leftChild, rightChild)
}
