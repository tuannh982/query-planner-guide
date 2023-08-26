package core.planner.volcano

import core.ctx.QueryExecutionContext
import core.planner.volcano.logicalplan.LogicalPlan
import core.planner.volcano.memo.{Group, Memo}
import core.ql
import utils.ctx.Context

class VolcanoPlannerContext(outer: QueryExecutionContext) extends Context {
  var query: ql.Statement   = _
  var rootPlan: LogicalPlan = _
  var rootGroup: Group      = _
  val memo: Memo            = new Memo()
}
