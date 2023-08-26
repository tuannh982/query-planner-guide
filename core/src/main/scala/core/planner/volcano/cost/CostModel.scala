package core.planner.volcano.cost

trait CostModel {
  def isBetter(currentCost: Cost, newCost: Cost): Boolean
}
