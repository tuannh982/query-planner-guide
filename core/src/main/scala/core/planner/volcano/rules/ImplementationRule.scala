package core.planner.volcano.rules

import core.planner.volcano.VolcanoPlannerContext
import core.planner.volcano.memo.GroupExpression
import core.planner.volcano.physicalplan.PhysicalPlan

trait ImplementationRule {
  def transform(expression: GroupExpression)(implicit ctx: VolcanoPlannerContext): Seq[PhysicalPlan]
}

object ImplementationRule {
  def combined: ImplementationRule = new ImplementationRule {
    override def transform(expression: GroupExpression)(implicit ctx: VolcanoPlannerContext): Seq[PhysicalPlan] = {
      ???
    }
  }
}
