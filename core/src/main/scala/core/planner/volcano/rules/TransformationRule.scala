package core.planner.volcano.rules

import core.planner.volcano.VolcanoPlannerContext
import core.planner.volcano.memo.GroupExpression
import core.planner.volcano.rules.transform.{ProjectionPushDown, X3TableJoinReorderBySize}

trait TransformationRule {
  def `match`(expression: GroupExpression)(implicit ctx: VolcanoPlannerContext): Boolean
  def transform(expression: GroupExpression)(implicit ctx: VolcanoPlannerContext): GroupExpression
}

object TransformationRule {

  val ruleBatches: Seq[Seq[TransformationRule]] = Seq(
    Seq(new ProjectionPushDown),
    Seq(new X3TableJoinReorderBySize)
  )
}
