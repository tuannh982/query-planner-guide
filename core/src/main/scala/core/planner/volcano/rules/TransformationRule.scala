package core.planner.volcano.rules

import core.planner.volcano.VolcanoPlannerContext
import core.planner.volcano.memo.GroupExpression

trait TransformationRule {
  def `match`(expression: GroupExpression)(implicit ctx: VolcanoPlannerContext): Boolean
  def transform(expression: GroupExpression)(implicit ctx: VolcanoPlannerContext): GroupExpression
}
