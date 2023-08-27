package core.planner.volcano.physicalplan.builder

import core.catalog.TableCatalog
import core.ctx.Connection
import core.execution.NormalScanOperator
import core.planner.volcano.cost.Cost
import core.planner.volcano.physicalplan.{Estimations, PhysicalPlan, PhysicalPlanBuilder, Scan}
import core.stats.TableStats

import scala.collection.mutable

class NormalScanImpl(
  connection: Connection,
  tableName: String,
  tableCatalog: TableCatalog,
  tableStats: TableStats,
  projection: Seq[String]
) extends PhysicalPlanBuilder {

  private val log2: Double = math.log(2)

  override def build(children: Seq[PhysicalPlan]): Option[PhysicalPlan] = {
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
    // populate traits
    val traits = mutable.ListBuffer[String]()
    tableCatalog.metadata.get("sorted") match {
      case Some(value) if value == "true" => traits += "SORTED"
      case _                              => (): Unit
    }
    Some(
      Scan(
        operator = NormalScanOperator(connection, tableName, tableCatalog, projection),
        cost = cost,
        estimations = estimations,
        traits = traits.toSet
      )
    )
  }
}
