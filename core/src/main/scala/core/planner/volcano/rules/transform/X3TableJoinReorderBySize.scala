package core.planner.volcano.rules.transform

import core.planner.volcano.VolcanoPlannerContext
import core.planner.volcano.logicalplan.{Join, LogicalPlan, Scan}
import core.planner.volcano.memo.GroupExpression
import core.planner.volcano.rules.TransformationRule

import scala.collection.mutable

class X3TableJoinReorderBySize extends TransformationRule {

  // check if the tree only contains SCAN and JOIN nodes, and also extract all SCAN nodes
  private def checkAndExtract(node: LogicalPlan, buffer: mutable.ListBuffer[Scan]): Boolean = {
    node match {
      case node @ Scan(_, _) =>
        buffer += node
        true
      case Join(left, right) => checkAndExtract(left, buffer) && checkAndExtract(right, buffer)
      case _                 => false
    }
  }

  override def `match`(expression: GroupExpression)(implicit ctx: VolcanoPlannerContext): Boolean = {
    val plan = expression.plan
    plan match {
      case node @ Join(_, _) =>
        val buffer = mutable.ListBuffer[Scan]()
        if (checkAndExtract(node, buffer)) {
          buffer.size == 3 // only match if the join is 3 tables join
        } else {
          false
        }
      case _ => false
    }
  }

  //noinspection ZeroIndexToHead
  override def transform(expression: GroupExpression)(implicit ctx: VolcanoPlannerContext): GroupExpression = {
    val plan   = expression.plan.asInstanceOf[Join]
    val buffer = mutable.ListBuffer[Scan]()
    checkAndExtract(plan, buffer)
    //
    val scans = buffer.toList
    implicit val ord: Ordering[Scan] = new Ordering[Scan] {
      override def compare(x: Scan, y: Scan): Int = {
        val xStats = ctx.statsProvider.tableStats(x.table.id)
        val yStats = ctx.statsProvider.tableStats(y.table.id)
        xStats.estimatedTableSize.compareTo(yStats.estimatedTableSize)
      }
    }
    val sorted  = scans.sorted
    val newPlan = Join(sorted(0), Join(sorted(1), sorted(2)))
    ctx.memo.getOrCreateGroupExpression(newPlan)
  }
}
