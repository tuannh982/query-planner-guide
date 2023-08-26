package core.planner.volcano.memo

import core.planner.volcano.logicalplan.LogicalPlan

import scala.collection.mutable

case class Group(
  id: Long,
  equivalents: mutable.HashSet[GroupExpression]
) extends ExplorationMark {
  val explorationMark: ExplorationMark = new ExplorationMark

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Group]

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: Group => that.canEqual(this) && equivalents == that.equivalents
      case _           => false
    }
  }

  override def hashCode(): Int = equivalents.hashCode()
}

case class GroupExpression(
  id: Long,
  plan: LogicalPlan,
  children: mutable.MutableList[Group]
) {
  val explorationMark: ExplorationMark = new ExplorationMark

  override def canEqual(that: Any): Boolean = that.isInstanceOf[GroupExpression]

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: GroupExpression => that.canEqual(this) && plan == that.plan && children == that.children
      case _                     => false
    }
  }

  override def hashCode(): Int = plan.hashCode()
}
