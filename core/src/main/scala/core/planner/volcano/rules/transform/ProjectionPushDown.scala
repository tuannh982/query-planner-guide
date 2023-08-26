package core.planner.volcano.rules.transform

import core.planner.volcano.VolcanoPlannerContext
import core.planner.volcano.logicalplan.{Join, LogicalPlan, Project, Scan}
import core.planner.volcano.memo.GroupExpression
import core.planner.volcano.rules.TransformationRule
import core.ql.{FieldID, TableID}

class ProjectionPushDown extends TransformationRule {
  private val All       = "*"
  private val AllFields = Seq(FieldID(TableID(All), All))

  override def `match`(expression: GroupExpression)(implicit ctx: VolcanoPlannerContext): Boolean = {
    val plan = expression.plan
    plan match {
      case Project(_, parent) => check(parent)
      case _                  => false
    }
  }

  // check if the tree only contains SCAN and JOIN nodes
  private def check(node: LogicalPlan): Boolean = {
    node match {
      case Scan(_, _)        => true
      case Join(left, right) => check(left) && check(right)
      case _                 => false
    }
  }

  override def transform(expression: GroupExpression)(implicit ctx: VolcanoPlannerContext): GroupExpression = {
    val plan    = expression.plan.asInstanceOf[Project]
    val newPlan = Project(AllFields, pushDown(plan.fields, plan.parent))
    ctx.memo.getOrCreateGroupExpression(newPlan)
  }

  private def pushDown(pushDownProjection: Seq[FieldID], node: LogicalPlan): LogicalPlan = {
    node match {
      case Scan(table, tableProjection) =>
        val filteredPushDownProjection = pushDownProjection.filter(_.table == table).map(_.id)
        val updatedProjection = if (filteredPushDownProjection.contains(All)) {
          Seq.empty
        } else {
          tableProjection ++ filteredPushDownProjection
        }
        Scan(table, updatedProjection)
      case Join(left, right) => Join(pushDown(pushDownProjection, left), pushDown(pushDownProjection, right))
      case _                 => throw new Exception("should never happen")
    }
  }
}
