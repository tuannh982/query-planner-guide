package core.planner

import core.catalog.{TableCatalog, TableCatalogProvider}
import core.ctx.Connection
import core.planner.volcano.cost.{Cost, CostModel}
import core.planner.volcano.{VolcanoPlanner, VolcanoPlannerContext}
import core.ql.QueryParser
import core.stats.{StatsProvider, TableStats}
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable

class VolcanoPlannerSpec extends AnyFlatSpec with MockFactory {
  behavior of "VolcanoPlanner"

  it should "correctly run without error" in {
    val in =
      """
        |SELECT
        | tbl1.id, tbl1.field1,
        | tbl2.id, tbl2.field1, tbl2.field2,
        | tbl3.id, tbl3.field2, tbl3.field2
        |FROM
        | tbl1
        | JOIN tbl2 ON tbl1.id = tbl2.id
        | JOIN tbl3 ON tbl2.id = tbl3.id
        |""".stripMargin
    val mockConnection = new Connection {
      override def fetchNextRow(table: String, projection: Seq[String]): Seq[Any] = Seq.empty // just mock
    }
    val mockTableCatalogProvider = new TableCatalogProvider {
      override def catalog(table: String): TableCatalog = table match {
        case "tbl1" =>
          TableCatalog(
            Seq(
              "id"     -> classOf[String],
              "field1" -> classOf[BigInt],
              "field2" -> classOf[BigDecimal],
              "field3" -> classOf[String],
              "field4" -> classOf[String]
            ),
            metadata = Map("sorted" -> "false")
          )
        case "tbl2" =>
          TableCatalog(
            Seq(
              "id"     -> classOf[String],
              "field1" -> classOf[BigInt],
              "field2" -> classOf[BigDecimal],
              "field3" -> classOf[String],
              "field4" -> classOf[String]
            ),
            metadata = Map("sorted" -> "true")
          )
        case "tbl3" =>
          TableCatalog(
            Seq(
              "id"     -> classOf[String],
              "field1" -> classOf[BigInt],
              "field2" -> classOf[BigDecimal],
              "field3" -> classOf[String],
              "field4" -> classOf[String]
            ),
            metadata = Map("sorted" -> "true")
          )
        case _ => throw new Exception("unrecognized table name")
      }
    }
    val mockStatsProvider = new StatsProvider {
      override def tableStats(table: String): TableStats = {
        table match {
          case "tbl1" =>
            TableStats(
              estimatedRowCount = 1000, // 1000
              avgColumnSize = Map(
                "id"     -> 200000, // 200_000 bytes
                "field1" -> 200000, // 200_000 bytes
                "field2" -> 200000, // 200_000 bytes
                "field3" -> 200000, // 200_000 bytes
                "field4" -> 200000  // 200_000 bytes
              )
            ) // avgRowSize 1_000_000 bytes, est total size: 1_000_000_000 bytes
          case "tbl2" =>
            TableStats(
              estimatedRowCount = 1000000L, // 1_000_000
              avgColumnSize = Map(
                "id"     -> 4, // 4 bytes
                "field1" -> 4, // 4 bytes
                "field2" -> 4, // 4 bytes
                "field3" -> 4, // 4 bytes
                "field4" -> 4  // 4 bytes
              )
            ) // avgRowSize 20 bytes, est total size: 20_000_000 bytes
          case "tbl3" =>
            TableStats(
              estimatedRowCount = 100000, // 100_000
              avgColumnSize = Map(
                "id"     -> 10000000, // 10_000_000 bytes
                "field1" -> 10000000, // 10_000_000 bytes
                "field2" -> 10000000, // 10_000_000 bytes
                "field3" -> 10000000, // 10_000_000 bytes
                "field4" -> 10000000  // 10_000_000 bytes
              )
            ) // avgRowSize 500 bytes, est total size: 50_000_000 bytes
          case _ => throw new Exception("unrecognized table name")
        }
      }
    }
    val mockCostModel = new CostModel {
      override def isBetter(currentCost: Cost, newCost: Cost): Boolean = {
        currentCost.estimatedCpuCost > newCost.estimatedCpuCost
      }
    }
    implicit val ctx: VolcanoPlannerContext = new VolcanoPlannerContext(
      mockConnection,
      mockTableCatalogProvider,
      mockStatsProvider,
      mockCostModel
    )
    val planner = new VolcanoPlanner
    QueryParser.parse(in) match {
      case Left(err) => fail(err)
      case Right(parsed) =>
        planner.initialize(parsed)
        planner.explore()
        planner.implement()
//        println(ctx.memo.showExplorationDiffMermaidViz(0, 1))
//        println(ctx.memo.showBestPhysicalPlanMermaidViz(ctx.rootGroup))
    }
  }

  it should "correctly execute without error" in {
    val in =
      """
        |SELECT
        | tbl1.id, tbl1.field1,
        | tbl2.field1, tbl2.field2,
        | tbl3.id, tbl3.field2, tbl3.field2
        |FROM
        | tbl1
        | JOIN tbl2 ON tbl1.id = tbl2.id
        | JOIN tbl3 ON tbl2.id = tbl3.id
        |""".stripMargin
    val defaultTableCatalog = TableCatalog(
      Seq("id" -> classOf[String], "field1" -> classOf[String], "field2" -> classOf[String]),
      metadata = Map("sorted" -> "true") // all table are sorted
    ) // all table will have the same spec
    val defaultTableStats = TableStats(
      estimatedRowCount = 3,
      avgColumnSize = Map("id" -> 10, "field1" -> 10, "field2" -> 10)
    ) // all table will have the same stats
    val mockTableCatalogProvider = new TableCatalogProvider {
      override def catalog(table: String): TableCatalog = defaultTableCatalog
    }
    val mockStatsProvider = new StatsProvider {
      override def tableStats(table: String): TableStats = defaultTableStats
    }
    val mockCostModel = new CostModel {
      override def isBetter(currentCost: Cost, newCost: Cost): Boolean = {
        currentCost.estimatedCpuCost > newCost.estimatedCpuCost
      }
    }
    val mockConnection: Connection = new Connection {
      val tbl1Data: Seq[Seq[Any]] = Seq(
        Seq("1", "1A", "1a"),
        Seq("1", "1B", "1b"),
        Seq("3", "1C", "1c")
      )

      val tbl2Data: Seq[Seq[Any]] = Seq(
        Seq("1", "2A", "2a"),
        Seq("1", "2B", "2b"),
        Seq("3", "2C", "2c")
      )

      val tbl3Data: Seq[Seq[Any]] = Seq(
        Seq("1", "3A", "3a"),
        Seq("2", "3B", "3b"),
        Seq("3", "3C", "3c")
      )

      var tbl1Idx = 0
      var tbl2Idx = 0
      var tbl3Idx = 0

      val columnIndices: Seq[(String, Int)] = Seq(
        "id"     -> 0,
        "field1" -> 1,
        "field2" -> 2
      )

      private def project(projection: Seq[String], table: String, row: Seq[Any]): Seq[Any] = {
        if (projection.length == 1 && (projection.head == "*" || projection.head == "*.*")) {
          row
        } else {
          val indices = projection.map { p =>
            columnIndices.find {
              case (field, i) =>
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

      override def fetchNextRow(table: String, projection: Seq[String]): Seq[Any] = table match {
        case "tbl1" =>
          if (tbl1Idx < tbl1Data.size) {
            val row = tbl1Data(tbl1Idx)
            tbl1Idx += 1
            project(projection, table, row)
          } else {
            null
          }
        case "tbl2" =>
          if (tbl2Idx < tbl2Data.size) {
            val row = tbl2Data(tbl2Idx)
            tbl2Idx += 1
            project(projection, table, row)
          } else {
            null
          }
        case "tbl3" =>
          if (tbl3Idx < tbl3Data.size) {
            val row = tbl3Data(tbl3Idx)
            tbl3Idx += 1
            project(projection, table, row)
          } else {
            null
          }
        case _ => throw new Exception("unrecognized table name")
      }
    }
    implicit val ctx: VolcanoPlannerContext = new VolcanoPlannerContext(
      mockConnection,
      mockTableCatalogProvider,
      mockStatsProvider,
      mockCostModel
    )
    val planner = new VolcanoPlanner
    QueryParser.parse(in) match {
      case Left(err) => fail(err)
      case Right(parsed) =>
        val operator      = planner.getPlan(parsed)
        var row: Seq[Any] = null
        println(operator.aliases())
        while ({ row = operator.next(); row != null }) {
          println(row)
        }
    }
  }
}
