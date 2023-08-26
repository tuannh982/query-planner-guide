package core.planner.volcano.physicalplan

import core.execution.Operator
import core.planner.volcano.cost.Cost

trait PhysicalPlan {
  def calculate(children: Seq[PhysicalPlan]): Unit // update this node children, cost, estimations, and physical operator
  def operator(): Operator
  def children(): Seq[PhysicalPlan]
  def cost(): Cost
  def estimations(): Estimations
}
