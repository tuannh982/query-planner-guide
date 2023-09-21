package core.execution

import core.catalog.TableCatalog
import core.ctx.Connection
import core.ql

import scala.collection.mutable

trait Operator {
  def aliases(): Seq[String]
  def next(): Option[Seq[Any]]
}

case class NormalScanOperator(
  connection: Connection,
  table: String,
  tableCatalog: TableCatalog,
  projection: Seq[String]
) extends Operator {

  override def aliases(): Seq[String] = {
    if (projection.isEmpty) {
      tableCatalog.columns.map {
        case (f, _) => s"$table.$f"
      }
    } else {
      projection.map { field =>
        s"$table.$field"
      }
    }
  }

  override def next(): Option[Seq[Any]] = {
    connection.fetchNextRow(table, projection)
  }
}

case class ProjectOperator(projection: Seq[ql.FieldID], child: Operator) extends Operator {

  private val projectionIndices: Seq[Int] = Utils.buildProjectionIndices(child.aliases(), aliases())

  override def aliases(): Seq[String] = {
    if (projection.length == 1 && (projection.head.table.id == "*" && projection.head.id == "*")) {
      child.aliases()
    } else {
      projection.map { field =>
        s"${field.table.id}.${field.id}"
      }
    }
  }

  override def next(): Option[Seq[Any]] = {
    child.next().map(Utils.project(_, projectionIndices))
  }
}

case class MergeJoinOperator(
  leftChild: Operator,
  rightChild: Operator,
  leftChildJoinFields: Seq[String],
  rightChildJoinFields: Seq[String]
) extends Operator {
  private var end                         = false
  private var bufferedRows: Seq[Seq[Any]] = Seq.empty
  private var prevLeft: Option[Seq[Any]]  = None
  private var prevRight: Option[Seq[Any]] = None

  private val leftProjectionIndices: Seq[Int] = Utils.buildProjectionIndices(
    leftChild.aliases(),
    leftChildJoinFields
  )

  private val rightProjectionIndices: Seq[Int] = Utils.buildProjectionIndices(
    rightChild.aliases(),
    rightChildJoinFields
  )
  override def aliases(): Seq[String] = leftChild.aliases() ++ rightChild.aliases()

  private def checkAndCompare(left: Option[Seq[Any]], right: Option[Seq[Any]], dir: Int = 0): Boolean = {
    (left, right) match {
      case (Some(left), Some(right)) =>
        if (dir == 0) {
          Utils.compare(left, right, leftProjectionIndices, rightProjectionIndices) == 0
        } else if (dir < 0) {
          Utils.compare(left, right, leftProjectionIndices, rightProjectionIndices) < 0
        } else {
          Utils.compare(left, right, leftProjectionIndices, rightProjectionIndices) > 0
        }
      case _ => false
    }
  }

  override def next(): Option[Seq[Any]] = {
    while (!end && bufferedRows.isEmpty) {
      var currentLeft  = prevLeft
      var currentRight = prevRight
      if (currentLeft.isEmpty) currentLeft = leftChild.next()
      if (currentRight.isEmpty) currentRight = rightChild.next()
      if (currentLeft.isEmpty || currentRight.isEmpty) {
        end = true
        return None
      }
      while (checkAndCompare(currentLeft, currentRight, -1)) currentLeft = leftChild.next()
      while (checkAndCompare(currentLeft, currentRight, 1)) currentRight = rightChild.next()
      val leftAnchor                                      = currentLeft
      val rightAnchor                                     = currentRight
      val leftBufferedRows: mutable.ListBuffer[Seq[Any]]  = mutable.ListBuffer()
      val rightBufferedRows: mutable.ListBuffer[Seq[Any]] = mutable.ListBuffer()
      while (checkAndCompare(currentLeft, rightAnchor)) {
        currentLeft.foreach(row => leftBufferedRows += row)
        currentLeft = leftChild.next()
      }
      while (checkAndCompare(leftAnchor, currentRight)) {
        currentRight.foreach(row => rightBufferedRows += row)
        currentRight = rightChild.next()
      }
      bufferedRows = for {
        l <- leftBufferedRows
        r <- rightBufferedRows
      } yield l ++ r
      prevLeft = currentLeft
      prevRight = currentRight
    }
    if (bufferedRows.nonEmpty) {
      val row = bufferedRows.head
      bufferedRows = bufferedRows.tail
      Option(row)
    } else {
      end = true
      None
    }
  }
}

case class HashJoinOperator(
  leftChild: Operator, // left child always be projected
  rightChild: Operator,
  leftAliases: Seq[String],
  rightAliases: Seq[String]
) extends Operator {
  override def aliases(): Seq[String] = leftChild.aliases() ++ rightChild.aliases()

  override def next(): Option[Seq[Any]] = {
    ??? // left unimplemented since it's just a demo project
  }
}
