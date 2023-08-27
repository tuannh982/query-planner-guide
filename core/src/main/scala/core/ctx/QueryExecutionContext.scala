package core.ctx

import utils.ctx.Context

trait QueryExecutionContext extends Context {
  def connection: Connection
}
