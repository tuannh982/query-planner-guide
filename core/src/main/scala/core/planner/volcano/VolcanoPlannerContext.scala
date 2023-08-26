package core.planner.volcano

import core.ctx.QueryExecutionContext
import core.planner.volcano.logicalplan.LogicalPlan
import core.planner.volcano.memo.{Group, Memo}
import core.planner.volcano.stats.StatsProvider
import core.ql

class VolcanoPlannerContext(val statsProvider: StatsProvider) extends QueryExecutionContext {
  var query: ql.Statement   = _
  var rootPlan: LogicalPlan = _
  var rootGroup: Group      = _
  val memo: Memo            = new Memo()
}
