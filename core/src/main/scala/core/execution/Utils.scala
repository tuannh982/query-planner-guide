package core.execution

import scala.collection.mutable

object Utils {

  def buildProjectionIndices(fields: Seq[String], projection: Seq[String]): Seq[Int] = {
    val indexed = fields.zipWithIndex
    if (projection.length == 1 && (projection.head == "*" || projection.head == "*.*")) {
      indexed.map(_._2)
    } else {
      projection.map { p =>
        val found = indexed.find {
          case (field, i) =>
            val split  = field.split('.')
            val pSplit = p.split('.')
            if (split.length == 2 && pSplit.length == 2) {
              p == field
            } else if (split.length == 2) {
              p == field || p == split(1)
            } else {
              p == field
            }
        }
        found match {
          case Some(value) => value
          case None        => throw new Exception(s"could not project $projection from $indexed")
        }
      }.map(_._2)
    }
  }

  def project(row: Seq[Any], indices: Seq[Int]): Seq[Any] = {
    val projected = mutable.ListBuffer[Any]()
    indices.foreach { i =>
      projected += row(i)
    }
    projected
  }

  def compare(
    left: Seq[Any],
    right: Seq[Any],
    leftProjectionIndices: Seq[Int],
    rightProjectionIndices: Seq[Int]
  ): Int = {
    if (leftProjectionIndices.size == rightProjectionIndices.size) {
      val leftProject  = Utils.project(left, leftProjectionIndices)
      val rightProject = Utils.project(right, rightProjectionIndices)
      val len          = leftProjectionIndices.size
      val leftKeys     = leftProject.map(_.toString)
      val rightKeys    = rightProject.map(_.toString)
      for (i <- 0 until len) {
        val leftKey  = leftKeys(i)
        val rightKey = rightKeys(i)
        val cmp      = leftKey.compareTo(rightKey)
        if (cmp != 0) {
          return cmp
        }
      }
      0
    } else {
      throw new Exception(
        s"leftProjectionIndices.size(${leftProjectionIndices.size}) is not equals to rightProjectionIndices.size(${rightProjectionIndices.size})"
      )
    }
  }
}
