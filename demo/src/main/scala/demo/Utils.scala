package demo

import core.catalog.TableCatalog
import core.execution.Operator
import core.stats.TableStats

import scala.collection.mutable

object Utils {

  case class Datasource(
    table: String,
    catalog: TableCatalog,
    rows: Seq[Seq[Any]],
    stats: TableStats
  ) {
    private val size         = rows.size
    private val fieldIndices = buildFieldIndices(catalog)
    private var fetchIdx     = 0

    def fetchNextRow(projection: Seq[String]): Option[Seq[Any]] = {
      if (fetchIdx < size) {
        val row = rows(fetchIdx)
        fetchIdx += 1
        Option(project(table, fieldIndices, projection, row))
      } else {
        None
      }
    }
  }

  def buildFieldIndices(catalog: TableCatalog): Map[String, Int] = {
    catalog.columns.zipWithIndex.map {
      case (tuple, i) => tuple._1 -> i
    }.toMap
  }

  def project(
    table: String,
    fieldIndices: Map[String, Int],
    projection: Seq[String],
    row: Seq[Any]
  ): Seq[Any] = {
    if (projection.isEmpty || (projection.length == 1 && (projection.head == "*" || projection.head == "*.*"))) {
      row
    } else {
      val indices = projection.map { p =>
        fieldIndices.find {
          case (field, _) =>
            val pSplit = p.split('.')
            if (pSplit.length == 2) {
              p == s"$table.$field"
            } else {
              p == field
            }
        }.get
      }.map(_._2)
      val projected = mutable.ListBuffer[Any]()
      indices.foreach { i =>
        projected += row(i)
      }
      projected
    }
  }

  def execute(operator: Operator): (Seq[String], Seq[Seq[Any]]) = {
    val header                = operator.aliases()
    val rows                  = mutable.ArrayBuffer[Seq[Any]]()
    var row: Option[Seq[Any]] = None
    while ({
      row = operator.next();
      row.isDefined
    }) {
      row.foreach(r => rows += Seq(r: _*))
    }
    (header, rows)
  }
}
