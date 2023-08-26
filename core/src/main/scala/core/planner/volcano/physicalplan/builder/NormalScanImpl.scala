package core.planner.volcano.physicalplan.builder

import core.execution.NormalScanOperator
import core.planner.volcano.cost.Cost
import core.planner.volcano.physicalplan.{Estimations, PhysicalPlan, PhysicalPlanBuilder, Scan}
import core.stats.TableStats

class NormalScanImpl(tableName: String, tableStats: TableStats, projection: Seq[String]) extends PhysicalPlanBuilder {

  override def build(children: Seq[PhysicalPlan]): PhysicalPlan = {
    val isNoProjection = projection.isEmpty || projection.head == "*"
    val (projectionCpuCostModifier, projectionMemoryCostModifier) = if (isNoProjection) {
      (1.0, 1.0)
    } else {
      (
        1.0 * projection.size / tableStats.avgColumnSize.size,
        1.0 * projection.map(tableStats.avgColumnSize(_)).sum / tableStats.avgRowSize
      )
    }
    val cost = Cost(
      estimatedCpuCost = projectionCpuCostModifier * tableStats.estimatedRowCount,
      estimatedMemoryCost = projectionMemoryCostModifier * tableStats.avgRowSize,
      estimatedTimeCost = tableStats.estimatedRowCount
    )
    val estimations = Estimations(
      estimatedLoopIterations = tableStats.estimatedRowCount,
      estimatedRowSize = tableStats.avgRowSize
    )
    Scan(
      operator = NormalScanOperator(tableName, projection),
      cost = cost,
      estimations = estimations
    )
  }
}
