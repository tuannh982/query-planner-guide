package core.execution

import core.catalog.TableCatalog
import core.ctx.Connection
import core.ql

import scala.collection.mutable

trait Operator {
  def aliases(): Seq[String]
  def next(): Seq[Any]
}

object Operator {

  def buildProjectionIndices(fields: Seq[String], projection: Seq[String]): Set[Int] = {
    val indexed = fields.zipWithIndex
    if (projection.length == 1 && (projection.head == "*" || projection.head == "*.*")) {
      indexed.map(_._2).toSet
    } else {
      projection.map { p =>
        indexed.find {
          case (field, i) =>
            val split = field.split('.')
            if (split.length == 2) {
              p == field || p == split(1)
            } else {
              p == field
            }
        }.get
      }.map(_._2).toSet
    }
  }

  def project(row: Seq[Any], indices: Set[Int]): Seq[Any] = {
    row.zipWithIndex.filter(v => indices.contains(v._2)).map(_._1)
  }
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
    val nextRow = connection.fetchNextRow(table)
    if (nextRow == null) {
      return null
    }
    val projectionIndices = projection.map(tableCatalog.fieldIndex).toSet // ignore any lookup error here
    val projected         = Operator.project(nextRow, projectionIndices)
    projected
  }
}

case class ProjectOperator(projection: Seq[ql.FieldID], child: Operator) extends Operator {

  private lazy val projectionIndices: Set[Int] = Operator.buildProjectionIndices(child.aliases(), aliases())

  override def aliases(): Seq[String] = {
    projection.map { field =>
      s"${field.table.id}.${field.id}"
    }
  }

  override def next(): Seq[Any] = {
    val nextRow = child.next()
    if (nextRow == null) {
      return null
    }
    val projected = Operator.project(nextRow, projectionIndices)
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

  private val leftProjectionIndices: Set[Int] = Operator.buildProjectionIndices(
    leftChild.aliases(),
    leftChildJoinFields
  )

  private val rightProjectionIndices: Set[Int] = Operator.buildProjectionIndices(
    rightChild.aliases(),
    rightChildJoinFields
  )

  private def compare(left: Seq[Any], right: Seq[Any]): Int = {
    val leftProject  = Operator.project(left, leftProjectionIndices)
    val rightProject = Operator.project(right, rightProjectionIndices)
    val leftK        = leftProject.map(_.toString).mkString("")
    val rightK       = rightProject.map(_.toString).mkString("")
    leftK.compareTo(rightK)
  }

  override def aliases(): Seq[String] = leftChild.aliases() ++ rightChild.aliases()

  private def checkAndCompare(left: Seq[Any], right: Seq[Any], dir: Int = 0): Boolean = {
    val base = left != null && right != null
    if (!base) return false
    if (dir == 0) {
      compare(left, right) == 0
    } else if (dir < 0) {
      compare(left, right) < 0
    } else {
      compare(left, right) > 0
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
      val cross = for {
        l <- leftBufferedRows
        r <- rightBufferedRows
      } yield (l, r)
      bufferedRows = cross.map {
        case (l, r) => l ++ r
      }
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
