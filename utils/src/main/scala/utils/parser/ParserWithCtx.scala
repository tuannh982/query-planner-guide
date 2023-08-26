package utils.parser

import utils.ctx.Context

trait ParserWithCtx[C <: Context, T] {
  def parse(in: String)(implicit ctx: C): Either[Throwable, T]
}
