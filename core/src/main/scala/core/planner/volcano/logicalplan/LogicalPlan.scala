package core.planner.volcano.logicalplan

import core.ql

sealed trait LogicalPlan {
  def describe(): String
  def children(): Seq[LogicalPlan]
}

case class Scan(table: ql.TableID, projection: Seq[String]) extends LogicalPlan {

  override def describe(): String = {
    if (projection.isEmpty) {
      s"SCAN ${table.id}"
    } else {
      s"SCAN ${table.id} (${projection.mkString(", ")})"
    }
  }

  override def children(): Seq[LogicalPlan] = Seq.empty
}

case class Project(fields: Seq[ql.FieldID], child: LogicalPlan) extends LogicalPlan {
  override def describe(): String = s"PROJECT ${fields.map(f => s"${f.table.id}.${f.id}").mkString(", ")}"

  override def canEqual(other: Any): Boolean = other.isInstanceOf[Project]

  override def equals(other: Any): Boolean = other match {
    case that: Project =>
      that.canEqual(this) &&
        fields.sortBy(x => (x.table.id, x.id)) == that.fields.sortBy(x => (x.table.id, x.id)) &&
        child == that.child
    case _ => false
  }

  override def children(): Seq[LogicalPlan] = Seq(child)
}

case class Join(left: LogicalPlan, right: LogicalPlan, on: Seq[(ql.FieldID, ql.FieldID)]) extends LogicalPlan {
  override def describe(): String = "JOIN"

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Join]

  override def equals(other: Any): Boolean = other match {
    case that: Join =>
      that.canEqual(this) && (
        left == that.left && right == that.right ||
          left == that.right && right == that.left
      )
    case _ => false
  }

  override def children(): Seq[LogicalPlan] = Seq(left, right)
}

object LogicalPlan {

  def toPlan(node: ql.Statement): LogicalPlan = {
    node match {
      case ql.Table(table)          => Scan(table, Seq.empty)
      case ql.Join(left, right, on) => Join(toPlan(left), toPlan(right), on)
      case ql.Select(fields, from)  => Project(fields, toPlan(from))
    }
  }
}
