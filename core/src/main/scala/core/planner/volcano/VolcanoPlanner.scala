package core.planner.volcano

import core.ctx.QueryExecutionContext
import core.execution.Operator
import core.planner.Planner
import core.ql.Statement

class VolcanoPlanner extends Planner {

  override def getPlan(expr: Statement)(implicit ctx: QueryExecutionContext): Operator = {
    implicit val plannerCtx: VolcanoPlannerContext = new VolcanoPlannerContext(ctx)
    // TODO
    ???
  }
}
