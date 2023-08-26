package core.planner

import core.catalog.{TableCatalog, TableCatalogProvider}
import core.planner.volcano.cost.{Cost, CostModel}
import core.planner.volcano.{VolcanoPlanner, VolcanoPlannerContext}
import core.ql.QueryParser
import core.stats.{StatsProvider, TableStats}
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

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
        | tbl1 JOIN tbl2 JOIN tbl3
        |""".stripMargin
    val mockTableCatalogProvider = new TableCatalogProvider {
      override def catalog(table: String): TableCatalog = table match {
        case "tbl1" =>
          TableCatalog(
            Map(
              "id"     -> classOf[String],
              "field1" -> classOf[BigInt],
              "field2" -> classOf[BigDecimal],
              "field3" -> classOf[String],
              "field4" -> classOf[String]
            )
          )
        case "tbl2" =>
          TableCatalog(
            Map(
              "id"     -> classOf[String],
              "field1" -> classOf[BigInt],
              "field2" -> classOf[BigDecimal],
              "field3" -> classOf[String],
              "field4" -> classOf[String]
            )
          )
        case "tbl3" =>
          TableCatalog(
            Map(
              "id"     -> classOf[String],
              "field1" -> classOf[BigInt],
              "field2" -> classOf[BigDecimal],
              "field3" -> classOf[String],
              "field4" -> classOf[String]
            )
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
        if (currentCost.estimatedMemoryCost == newCost.estimatedMemoryCost) {
          if (currentCost.estimatedCpuCost == newCost.estimatedCpuCost) {
            currentCost.estimatedTimeCost > newCost.estimatedTimeCost
          } else {
            currentCost.estimatedCpuCost > newCost.estimatedCpuCost
          }
        } else {
          currentCost.estimatedMemoryCost > newCost.estimatedMemoryCost
        }
      }
    }
    implicit val ctx: VolcanoPlannerContext = new VolcanoPlannerContext(
      mockTableCatalogProvider,
      mockStatsProvider,
      mockCostModel
    )
    val planner = new VolcanoPlanner
    QueryParser.parse(in) match {
      case Left(err) => fail(err)
      case Right(parsed) =>
        planner.getPlan(parsed)
        planner.initialize(parsed)
        planner.explore()
        planner.implement()
    }
  }
}
