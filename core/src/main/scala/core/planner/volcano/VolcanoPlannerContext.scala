package core.planner.volcano

import core.catalog.TableCatalogProvider
import core.ctx.QueryExecutionContext
import core.planner.volcano.cost.CostModel
import core.planner.volcano.logicalplan.LogicalPlan
import core.planner.volcano.memo.{Group, Memo}
import core.ql
import core.stats.StatsProvider

class VolcanoPlannerContext(
  val tableCatalogProvider: TableCatalogProvider,
  val statsProvider: StatsProvider,
  val costModel: CostModel
) extends QueryExecutionContext {
  var query: ql.Statement   = _
  var rootPlan: LogicalPlan = _
  var rootGroup: Group      = _
  val memo: Memo            = new Memo()
}
