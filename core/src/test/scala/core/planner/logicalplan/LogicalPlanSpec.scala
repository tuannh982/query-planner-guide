package core.planner.logicalplan

import core.ctx.{Connection, QueryExecutionContext}
import core.planner.volcano.logicalplan.{Join, LogicalPlan, Project, Scan}
import core.ql
import core.ql.{FieldID, QueryParser}
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class LogicalPlanSpec extends AnyFlatSpec with MockFactory {
  behavior of "LogicalPlan"

  it should "correctly parse query and convert to logical plan (1)" in {
    val in =
      """
        |SELECT
        | tbl1.id, tbl1.field1,
        | tbl2.id, tbl2.field1, tbl2.field2,
        | tbl3.id, tbl3.field2, tbl3.field2
        |FROM
        | tbl1 JOIN tbl2 JOIN tbl3
        |""".stripMargin
    val mockConnection = new Connection {
      override def fetchNextRow(table: String): Seq[Any] = Seq.empty // just mock
    }
    implicit val ctx: QueryExecutionContext = new QueryExecutionContext {
      override def connection: Connection = mockConnection
    }
    QueryParser.parse(in) match {
      case Left(err) => fail(err)
      case Right(parsed) =>
        val plan = LogicalPlan.toPlan(parsed)
        assert(
          plan == Project(
            Seq(
              FieldID(ql.TableID("tbl1"), "id"),
              FieldID(ql.TableID("tbl1"), "field1"),
              FieldID(ql.TableID("tbl2"), "id"),
              FieldID(ql.TableID("tbl2"), "field1"),
              FieldID(ql.TableID("tbl2"), "field2"),
              FieldID(ql.TableID("tbl3"), "id"),
              FieldID(ql.TableID("tbl3"), "field2"),
              FieldID(ql.TableID("tbl3"), "field2")
            ),
            Join(
              Scan(ql.TableID("tbl1"), Seq.empty),
              Join(
                Scan(ql.TableID("tbl2"), Seq.empty),
                Scan(ql.TableID("tbl3"), Seq.empty)
              )
            )
          )
        )
    }
  }
}
