package core.execution

import core.catalog.TableCatalog
import core.ctx.Connection
import core.ql

import scala.collection.mutable

trait Operator {
  def aliases(): Seq[String]
  def next(): Seq[Any]
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

  override def next(): Seq[Any] = {
    val nextRow = connection.fetchNextRow(table, projection)
    if (nextRow == null) {
      return null
    }
    nextRow
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

  override def next(): Seq[Any] = {
    val nextRow = child.next()
    if (nextRow == null) {
      return null
    }
    val projected = Utils.project(nextRow, projectionIndices)
    projected
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
  private var prevLeft: Seq[Any]          = _
  private var prevRight: Seq[Any]         = _

  private val leftProjectionIndices: Seq[Int] = Utils.buildProjectionIndices(
    leftChild.aliases(),
    leftChildJoinFields
  )

  private val rightProjectionIndices: Seq[Int] = Utils.buildProjectionIndices(
    rightChild.aliases(),
    rightChildJoinFields
  )
  override def aliases(): Seq[String] = leftChild.aliases() ++ rightChild.aliases()

  private def checkAndCompare(left: Seq[Any], right: Seq[Any], dir: Int = 0): Boolean = {
    val base = left != null && right != null
    if (!base) return false
    if (dir == 0) {
      Utils.compare(left, right, leftProjectionIndices, rightProjectionIndices) == 0
    } else if (dir < 0) {
      Utils.compare(left, right, leftProjectionIndices, rightProjectionIndices) < 0
    } else {
      Utils.compare(left, right, leftProjectionIndices, rightProjectionIndices) > 0
    }
  }

  override def next(): Seq[Any] = {
    while (!end && bufferedRows.isEmpty) {
      var currentLeft  = prevLeft
      var currentRight = prevRight
      if (currentLeft == null) currentLeft = leftChild.next()
      if (currentRight == null) currentRight = rightChild.next()
      if (currentLeft == null || currentRight == null) {
        end = true
        return null
      }
      while (checkAndCompare(currentLeft, currentRight, -1)) currentLeft = leftChild.next()
      while (checkAndCompare(currentLeft, currentRight, 1)) currentRight = rightChild.next()
      val leftAnchor                                      = currentLeft
      val rightAnchor                                     = currentRight
      val leftBufferedRows: mutable.ListBuffer[Seq[Any]]  = mutable.ListBuffer()
      val rightBufferedRows: mutable.ListBuffer[Seq[Any]] = mutable.ListBuffer()
      while (checkAndCompare(currentLeft, rightAnchor)) {
        leftBufferedRows += currentLeft
        currentLeft = leftChild.next()
      }
      while (checkAndCompare(leftAnchor, currentRight)) {
        rightBufferedRows += currentRight
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
      row
    } else {
      end = true
      null
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

  override def next(): Seq[Any] = {
    ??? // left unimplemented since it's just a demo project
  }
}
