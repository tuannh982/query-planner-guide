package core.planner

import core.planner.volcano.stats.{StatsProvider, TableStats}
import core.planner.volcano.{VolcanoPlanner, VolcanoPlannerContext}
import core.ql.QueryParser
import core.utils.visualization.MemoVizUtils.Mermaid
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class VolcanoPlannerSpec extends AnyFlatSpec with MockFactory {
  behavior of "VolcanoPlanner"

  it should "correctly explore" in {
    val in =
      """
        |SELECT
        | tbl1.id, tbl1.field1,
        | tbl2.id, tbl2.field1, tbl2.field2,
        | tbl3.id, tbl3.field2, tbl3.field2
        |FROM
        | tbl1 JOIN tbl2 JOIN tbl3
        |""".stripMargin
    val statsProvider = new StatsProvider {
      override def tableStats(table: String): TableStats = {
        table match {
          case "tbl1" =>
            TableStats(
              estimatedRowCount = 1000, // 1000
              avgColumnSize = Map(
                "f1" -> 200000, // 200_000 bytes
                "f2" -> 200000, // 200_000 bytes
                "f3" -> 200000, // 200_000 bytes
                "f4" -> 200000, // 200_000 bytes
                "f5" -> 200000  // 200_000 bytes
              )
            ) // avgRowSize 1_000_000 bytes, est total size: 1_000_000_000 bytes
          case "tbl2" =>
            TableStats(
              estimatedRowCount = 1000000L, // 1_000_000
              avgColumnSize = Map(
                "f1" -> 4, // 4 bytes
                "f2" -> 4, // 4 bytes
                "f3" -> 4, // 4 bytes
                "f4" -> 4, // 4 bytes
                "f5" -> 4  // 4 bytes
              )
            ) // avgRowSize 20 bytes, est total size: 20_000_000 bytes
          case "tbl3" =>
            TableStats(
              estimatedRowCount = 100000, // 100_000
              avgColumnSize = Map(
                "f1" -> 10000000, // 10_000_000 bytes
                "f2" -> 10000000, // 10_000_000 bytes
                "f3" -> 10000000, // 10_000_000 bytes
                "f4" -> 10000000, // 10_000_000 bytes
                "f5" -> 10000000  // 10_000_000 bytes
              )
            ) // avgRowSize 500 bytes, est total size: 50_000_000 bytes
          case _ => throw new Exception("unrecognized table name")
        }
      }
    }
    implicit val ctx: VolcanoPlannerContext = new VolcanoPlannerContext(statsProvider)
    val planner                             = new VolcanoPlanner
    QueryParser.parse(in) match {
      case Left(err) => fail(err)
      case Right(parsed) =>
        planner.initialize(parsed)
        planner.explore()
        println(ctx.memo.showDiffMermaidViz(0, 2))
    }
  }
}
