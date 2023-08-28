package core.planner.volcano.rules.transform

import core.planner.volcano.VolcanoPlannerContext
import core.planner.volcano.logicalplan.{Join, LogicalPlan, Project, Scan}
import core.planner.volcano.memo.GroupExpression
import core.planner.volcano.rules.TransformationRule
import core.ql.FieldID

import scala.collection.mutable

class ProjectionPushDown extends TransformationRule {

  override def `match`(expression: GroupExpression)(implicit ctx: VolcanoPlannerContext): Boolean = {
    val plan = expression.plan
    plan match {
      case Project(_, child) => check(child)
      case _                 => false
    }
  }

  // check if the tree only contains SCAN and JOIN nodes
  private def check(node: LogicalPlan): Boolean = {
    node match {
      case Scan(_, _)           => true
      case Join(left, right, _) => check(left) && check(right)
      case _                    => false
    }
  }

  private def extractProjections(node: LogicalPlan, buffer: mutable.ListBuffer[FieldID]): Unit = {
    node match {
      case Scan(_, _) => (): Unit
      case Project(fields, parent) =>
        buffer ++= fields
        extractProjections(parent, buffer)
      case Join(left, right, on) =>
        buffer ++= on.map(_._1) ++ on.map(_._2)
        extractProjections(left, buffer)
        extractProjections(right, buffer)
    }
  }

  override def transform(expression: GroupExpression)(implicit ctx: VolcanoPlannerContext): GroupExpression = {
    val plan               = expression.plan.asInstanceOf[Project]
    val pushDownProjection = mutable.ListBuffer[FieldID]()
    extractProjections(plan, pushDownProjection)
    val newPlan = Project(plan.fields, pushDown(pushDownProjection.distinct, plan.child))
    ctx.memo.getOrCreateGroupExpression(newPlan)
  }

  private def pushDown(pushDownProjection: Seq[FieldID], node: LogicalPlan): LogicalPlan = {
    node match {
      case Scan(table, tableProjection) =>
        val filteredPushDownProjection = pushDownProjection.filter(_.table == table).map(_.id)
        val updatedProjection =
          if (filteredPushDownProjection.contains("*") || filteredPushDownProjection.contains("*.*")) {
            Seq.empty
          } else {
            (tableProjection ++ filteredPushDownProjection).distinct
          }
        Scan(table, updatedProjection)
      case Join(left, right, on) => Join(pushDown(pushDownProjection, left), pushDown(pushDownProjection, right), on)
      case _                     => throw new Exception("should never happen")
    }
  }
}
