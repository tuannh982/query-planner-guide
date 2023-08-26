package core.planner

import core.execution.Operator
import core.ql.Statement

trait Planner {
  def getPlan(expr: Statement): Operator
}
