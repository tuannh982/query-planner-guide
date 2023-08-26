package core.planner

import core.ctx.QueryExecutionContext
import core.execution.Operator
import core.ql.Statement

trait Planner {
  def getPlan(expr: Statement)(implicit ctx: QueryExecutionContext): Operator
}
