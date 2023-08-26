package core.planner

import core.ctx.QueryExecutionContext
import core.execution.Operator
import core.ql.Statement

trait Planner[C <: QueryExecutionContext] {
  def getPlan(expr: Statement)(implicit ctx: C): Operator
}
