package demo

import core.catalog.{TableCatalog, TableCatalogProvider}
import core.ctx.Connection
import core.planner.volcano.cost.{Cost, CostModel}
import core.planner.volcano.{VolcanoPlanner, VolcanoPlannerContext}
import core.ql.QueryParser
import core.stats.{StatsProvider, TableStats}
import demo.Utils.Datasource

object DataSources {

  val table1: Datasource = Datasource(
    table = "emp",
    catalog = TableCatalog(
      Seq(
        "id"   -> classOf[String],
        "code" -> classOf[String]
      ),
      metadata = Map("sorted" -> "true") // assumes rows are already sorted by id
    ),
    rows = Seq(
      Seq("1", "Emp A"),
      Seq("2", "Emp B"),
      Seq("3", "Emp C")
    ),
    stats = TableStats(
      estimatedRowCount = 3,
      avgColumnSize = Map("id" -> 10, "code" -> 32)
    )
  )

  val table2: Datasource = Datasource(
    table = "dept",
    catalog = TableCatalog(
      Seq(
        "emp_id"    -> classOf[String],
        "dept_name" -> classOf[String]
      ),
      metadata = Map("sorted" -> "true") // assumes rows are already sorted by emp_id (this is just a fake trait to demonstrate how trait works)
    ),
    rows = Seq(
      Seq("1", "Dept 1"),
      Seq("1", "Dept 2"),
      Seq("2", "Dept 3"),
      Seq("3", "Dept 3")
    ),
    stats = TableStats(
      estimatedRowCount = 4,
      avgColumnSize = Map("emp_id" -> 10, "dept_name" -> 255)
    )
  )

  val table3: Datasource = Datasource(
    table = "emp_info",
    catalog = TableCatalog(
      Seq(
        "id"     -> classOf[String],
        "name"   -> classOf[String],
        "origin" -> classOf[String]
      ),
      metadata = Map("sorted" -> "true") // assumes rows are already sorted by id (this is just a fake trait to demonstrate how trait works)
    ),
    rows = Seq(
      Seq("1", "AAAAA", "Country A"),
      Seq("2", "BBBBB", "Country A"),
      Seq("3", "CCCCC", "Country B")
    ),
    stats = TableStats(
      estimatedRowCount = 3,
      avgColumnSize = Map("id" -> 10, "name" -> 255, "origin" -> 255)
    )
  )

  val tables: Seq[Datasource]           = Seq(table1, table2, table3)
  val tableMap: Map[String, Datasource] = tables.map(e => e.table -> e).toMap
}

object Demo {
  val catalogs: TableCatalogProvider = (table: String) => DataSources.tableMap(table).catalog

  val statsProvider: StatsProvider = (table: String) => DataSources.tableMap(table).stats

  val connection: Connection = (table: String, projection: Seq[String]) => {
    DataSources.tableMap(table).fetchNextRow(projection)
  }

  val costModel: CostModel = (currentCost: Cost, newCost: Cost) => {
    currentCost.estimatedCpuCost > newCost.estimatedCpuCost
  }

  implicit val plannerContext: VolcanoPlannerContext =
    new VolcanoPlannerContext(connection, catalogs, statsProvider, costModel)

  def main(args: Array[String]): Unit = {
    val query =
      """
        |SELECT
        | emp.id, emp.code, dept.dept_name, emp_info.name, emp_info.origin
        |FROM
        | emp
        | JOIN dept ON emp.id = dept.emp_id
        | JOIN emp_info ON dept.emp_id = emp_info.id
        |""".stripMargin
    val planner = new VolcanoPlanner
    QueryParser.parse(query) match {
      case Left(err) => throw err
      case Right(parsed) =>
        val operator = planner.getPlan(parsed)
        val result   = Utils.execute(operator)
        // print result
        println(result._1.mkString(","))
        result._2.foreach(row => println(row.mkString(",")))
    }
  }
}
