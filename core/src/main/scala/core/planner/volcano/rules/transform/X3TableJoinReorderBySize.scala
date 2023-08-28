package core.planner.volcano.rules.transform

import core.planner.volcano.VolcanoPlannerContext
import core.planner.volcano.logicalplan.{Join, LogicalPlan, Scan}
import core.planner.volcano.memo.GroupExpression
import core.planner.volcano.rules.TransformationRule
import core.ql

import scala.collection.mutable

class X3TableJoinReorderBySize extends TransformationRule {

  // check if the tree only contains SCAN and JOIN nodes, and also extract all SCAN nodes and JOIN conditions
  private def checkAndExtract(
    node: LogicalPlan,
    buffer: mutable.ListBuffer[Scan],
    joinCondBuffer: mutable.ListBuffer[(ql.FieldID, ql.FieldID)]
  ): Boolean = {
    node match {
      case node @ Scan(_, _) =>
        buffer += node
        true
      case Join(left, right, on) =>
        joinCondBuffer ++= on
        checkAndExtract(left, buffer, joinCondBuffer) && checkAndExtract(right, buffer, joinCondBuffer)
      case _ => false
    }
  }

  private def buildInterchangeableJoinCond(conditions: Seq[(ql.FieldID, ql.FieldID)]): Seq[Seq[ql.FieldID]] = {
    val buffer = mutable.ListBuffer[mutable.Set[ql.FieldID]]()
    conditions.foreach { cond =>
      val set = buffer.find { set =>
        set.contains(cond._1) || set.contains(cond._2)
      } match {
        case Some(set) => set
        case None =>
          val set = mutable.Set[ql.FieldID]()
          buffer += set
          set
      }
      set += cond._1
      set += cond._2
    }
    buffer.map(_.toSeq)
  }

  override def `match`(expression: GroupExpression)(implicit ctx: VolcanoPlannerContext): Boolean = {
    val plan = expression.plan
    plan match {
      case node @ Join(_, _, _) =>
        val buffer         = mutable.ListBuffer[Scan]()
        val joinCondBuffer = mutable.ListBuffer[(ql.FieldID, ql.FieldID)]()
        if (checkAndExtract(node, buffer, joinCondBuffer)) {
          // only match if the join is 3 tables join
          if (buffer.size == 3) {
            var check               = true
            val interChangeableCond = buildInterchangeableJoinCond(joinCondBuffer)
            interChangeableCond.foreach { c =>
              check &= c.size == 3
            }
            check
          } else {
            false
          }
        } else {
          false
        }
      case _ => false
    }
  }

  //noinspection ZeroIndexToHead
  override def transform(expression: GroupExpression)(implicit ctx: VolcanoPlannerContext): GroupExpression = {
    val plan           = expression.plan.asInstanceOf[Join]
    val buffer         = mutable.ListBuffer[Scan]()
    val joinCondBuffer = mutable.ListBuffer[(ql.FieldID, ql.FieldID)]()
    checkAndExtract(plan, buffer, joinCondBuffer)
    val interChangeableCond = buildInterchangeableJoinCond(joinCondBuffer)
    //
    val scans = buffer.toList
    implicit val ord: Ordering[Scan] = new Ordering[Scan] {
      override def compare(x: Scan, y: Scan): Int = {
        val xStats = ctx.statsProvider.tableStats(x.table.id)
        val yStats = ctx.statsProvider.tableStats(y.table.id)
        xStats.estimatedTableSize.compareTo(yStats.estimatedTableSize)
      }
    }
    def getJoinCond(left: Scan, right: Scan): Seq[(ql.FieldID, ql.FieldID)] = {
      val leftFields = interChangeableCond.flatMap { c =>
        c.filter(p => p.table == left.table)
      }
      val rightFields = interChangeableCond.flatMap { c =>
        c.filter(p => p.table == right.table)
      }
      if (leftFields.length != rightFields.length) {
        throw new Exception(s"leftFields.length(${leftFields.length}) != rightFields.length(${rightFields.length})")
      } else {
        leftFields zip rightFields
      }
    }
    val sorted = scans.sorted
    val newPlan = Join(
      sorted(0),
      Join(
        sorted(1),
        sorted(2),
        getJoinCond(sorted(1), sorted(2))
      ),
      getJoinCond(sorted(0), sorted(1))
    )
    ctx.memo.getOrCreateGroupExpression(newPlan)
  }
}
