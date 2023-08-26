package core.planner

import core.ctx.QueryExecutionContext
import core.planner.volcano.{VolcanoPlanner, VolcanoPlannerContext}
import core.ql.QueryParser
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
    implicit val ctx: QueryExecutionContext        = new QueryExecutionContext
    implicit val plannerCtx: VolcanoPlannerContext = new VolcanoPlannerContext(ctx)
    val planner                                    = new VolcanoPlanner
    QueryParser.parse(in) match {
      case Left(err) => fail(err)
      case Right(parsed) =>
        planner.initialize(parsed)
        planner.explore()
    }
  }
}
