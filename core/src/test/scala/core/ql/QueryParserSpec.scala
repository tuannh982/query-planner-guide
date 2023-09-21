package core.ql

import core.ctx.{Connection, QueryExecutionContext}
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class QueryParserSpec extends AnyFlatSpec with MockFactory {
  behavior of "QueryParser"

  it should "correctly parse query (1)" in {
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
      override def fetchNextRow(table: String, projection: Seq[String]): Option[Seq[Any]] = None // just mock
    }
    implicit val ctx: QueryExecutionContext = new QueryExecutionContext {
      override def connection: Connection = mockConnection
    }
    QueryParser.parse(in) match {
      case Left(err) => fail(err)
      case Right(parsed) =>
        assert(
          parsed == Select(
            Seq(
              FieldID(TableID("tbl1"), "id"),
              FieldID(TableID("tbl1"), "field1"),
              FieldID(TableID("tbl2"), "id"),
              FieldID(TableID("tbl2"), "field1"),
              FieldID(TableID("tbl2"), "field2"),
              FieldID(TableID("tbl3"), "id"),
              FieldID(TableID("tbl3"), "field2"),
              FieldID(TableID("tbl3"), "field2")
            ),
            Join(
              Table(TableID("tbl1")),
              Join(
                Table(TableID("tbl2")),
                Table(TableID("tbl3")),
                Seq(
                  FieldID(TableID("tbl2"), "id") -> FieldID(TableID("tbl3"), "id"),
                )
              ),
              Seq(
                FieldID(TableID("tbl1"), "id") -> FieldID(TableID("tbl2"), "id"),
              )
            )
          )
        )
    }
  }
}
