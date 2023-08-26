package core.planner.logicalplan

import core.ql
import utils.list.{FixedList, List0, List1, List2}

sealed trait LogicalPlan {
  def children(): FixedList[LogicalPlan]
}

case class Scan(table: ql.TableID, projection: Seq[String]) extends LogicalPlan {

  override def toString: String = {
    if (projection.isEmpty) {
      s"SCAN ${table.id}"
    } else {
      s"SCAN ${table.id} (${projection.mkString(", ")})"
    }
  }

  override def children(): FixedList[LogicalPlan] = List0()
}

case class Project(fields: Seq[ql.FieldID], parent: LogicalPlan) extends LogicalPlan {
  override def toString: String = s"PROJECT ${fields.map(_.toString).mkString(", ")}"

  override def canEqual(other: Any): Boolean = other.isInstanceOf[Project]

  override def equals(other: Any): Boolean = other match {
    case that: Project =>
      (that canEqual this) &&
        fields.sortBy(x => (x.table.id, x.id)) == that.fields.sortBy(x => (x.table.id, x.id)) &&
        parent == that.parent
    case _ => false
  }

  override def children(): FixedList[LogicalPlan] = List1(parent)
}

case class Join(left: LogicalPlan, right: LogicalPlan) extends LogicalPlan {
  override def toString: String = "JOIN"

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Join]

  override def equals(other: Any): Boolean = other match {
    case that: Join =>
      (that canEqual this) &&
        (left == that.left && right == that.right || left == that.right && right == that.left)
    case _ => false
  }

  override def children(): FixedList[LogicalPlan] = List2(left, right)
}

object LogicalPlan {

  def toPlan(node: ql.Statement): LogicalPlan = {
    node match {
      case ql.Table(table)         => Scan(table, Seq.empty)
      case ql.Join(left, right)    => Join(toPlan(left), toPlan(right))
      case ql.Select(fields, from) => Project(fields, toPlan(from))
    }
  }
}
