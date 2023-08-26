package core.planner.volcano.cost

case class Cost(
  estimatedCpuCost: Double,
  estimatedMemoryCost: Double,
  estimatedTimeCost: Double
) {

  override def toString: String = {
    "Cost(cpu=%.2f, mem=%.2f, time=%.2f)".format(estimatedCpuCost, estimatedMemoryCost, estimatedTimeCost)
  }
}
