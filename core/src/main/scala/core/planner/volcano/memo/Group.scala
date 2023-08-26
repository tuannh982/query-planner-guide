package core.planner.volcano.memo

import core.planner.volcano.logicalplan.LogicalPlan

import scala.collection.mutable

case class Group(
  id: Long,
  equivalents: mutable.HashSet[GroupExpression]
)

case class GroupExpression(
  id: Long,
  plan: LogicalPlan,
  children: mutable.MutableList[Group]
) {

  override def canEqual(that: Any): Boolean = that.isInstanceOf[GroupExpression]

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: GroupExpression => that.canEqual(this) && plan == that.plan && children == that.children
      case _                     => false
    }
  }

  override def hashCode(): Int = plan.hashCode()
}
