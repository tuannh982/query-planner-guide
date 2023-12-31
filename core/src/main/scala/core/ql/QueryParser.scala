package core.ql

import core.ctx.QueryExecutionContext
import utils.parser.ParserWithCtx

import scala.util.Try
import scala.util.parsing.combinator.RegexParsers

object QueryParser extends ParserWithCtx[QueryExecutionContext, Statement] with RegexParsers {

  override def parse(in: String)(implicit ctx: QueryExecutionContext): Either[Throwable, Statement] = {
    Try(parseAll(statement, in) match {
      case Success(result, _) => Right(result)
      case NoSuccess(msg, _)  => Left(new Exception(msg))
    }) match {
      case util.Failure(ex)    => Left(ex)
      case util.Success(value) => value
    }
  }

  // common
  private def str: Parser[String]     = """[a-zA-Z0-9_]+""".r
  private def fqdnStr: Parser[String] = """[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+""".r

  // identifier
  private def tableId: Parser[TableID] = str ^^ (s => TableID(s))

  private def fieldId: Parser[FieldID] = fqdnStr ^^ { s =>
    val identifiers = s.split('.')
    if (identifiers.length != 2) {
      throw new Exception("should never happen")
    } else {
      val table = identifiers.head
      val field = identifiers(1)
      FieldID(TableID(table), field)
    }
  }

  // statement
  private def table: Parser[Table]          = tableId ^^ (t => Table(t))
  private def subQuery: Parser[Statement]   = "(" ~> select <~ ")"
  private def fromSource: Parser[Statement] = table ||| subQuery

  private def select: Parser[Select] =
    "SELECT" ~ rep1sep(fieldId, ",") ~ "FROM" ~ fromSource ~ rep(
      "JOIN" ~ fromSource ~ "ON" ~ rep1(fieldId ~ "=" ~ fieldId)
    ) ^^ {
      case _ ~ fields ~ _ ~ src ~ joins =>
        val p = if (joins.nonEmpty) {
          def chain(left: Statement, right: Seq[(Statement, Seq[(FieldID, FieldID)])]): Join = {
            if (right.isEmpty) {
              throw new Exception("should never happen")
            } else if (right.length == 1) {
              val next = right.head
              Join(left, next._1, next._2)
            } else {
              val next = right.head
              Join(left, chain(next._1, right.tail), next._2)
            }
          }
          val temp = joins.map { join =>
            val statement = join._1._1._2
            val joinOn    = join._2.map(on => on._1._1 -> on._2)
            statement -> joinOn
          }
          chain(src, temp)
        } else {
          src
        }
        Select(fields, p)
    }

  private def statement: Parser[Statement] = select
}
