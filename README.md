[placeholder]

## Introduction

A query planner is a component of a database management system (DBMS) that is responsible for generating a plan for
executing a database query. The query plan specifies the steps that the DBMS will take to retrieve the data requested by
the query. The goal of the query planner is to generate a plan that is as efficient as possible, meaning that it will
return the data to the user as quickly as possible.

Query planners are complex pieces of software, and they can be difficult to understand. This guide to implementing a
cost-based query planner will provide you with a step-by-step overview of the process, how to implement your own
cost-based query planner, while still cover the basic concepts of query planner.

> Written by AI, edited by human

## Targeted audiences

This guide is written for:

- who used to work with query engines
- who curious, want to make their own stuffs
- who wants to learn DB stuffs but hate math

Goals:

- Able to understand the basic of query planning
- Able to write your own query planner

## Basic architecture of a query engine

```mermaid
graph TD
    user((user))
    parser[Query Parser]
    planner[Query Planner]
    executor[Query Processor]
    user -- text query --> parser
    parser -- AST --> planner
    planner -- physical plan --> executor
```

Basic architecture of a query engine is consisted of those components:

- **Query parser:** used to parse user query input, usually in human-readable text format (such as SQL)
- **Query planner:** used to generate the plan/strategy to execute the query. Normally the query planner will choose the
  best plan among several plans generated from a single query
- **Query processor:** used to execute the query plan, which is output by the query planner

## Types of query planners

Normally, query planners are divided into 2 types:

- heuristic planner
- cost-based planner

Heuristic planner is the query planner which used pre-defined rules to generate query plan.

Cost-based planner is the query planner who based on the cost to generate query, it tries to find the optimal plan based
on cost of the input query.

While heuristic planner usually find the best plan by apply transform rules if it knows that the transformed plan is
better, the cost-based planner find the best plan by enumerate equivalent plans and try to find the best plan among
them.

### Cost based query planner

In cost based query planner, it's usually composed of phases:

- Plan Enumerations
- Query Optimization

In the Plan Enumerations phase, the planner will enumerate the possible equivalent plans.

After that, in Query Optimization phase, the planner will search for the best plan from the list of enumerated plans.
The best plan is the plan having the lowest cost, which the cost model (or cost function) is defined.

Because the natural of logical plan, is having tree-like structure, so you can think the optimization/search is actually
a tree-search problem. And there are lots of tree-search algorithms out here:

- Exhaustive search, such as deterministic dynamic programming. The algorithm will perform searching for best plan until
  search termination conditions
- Randomized search, such as randomized tree search. The algorithm will perform searching for best plan until
  search termination conditions

**notes:** in theory it's possible to use any kind of tree-search algorithm. However, in practical it's not feasible
since the
search time is increased when our search algorithm is complex

**notes:** the search termination conditions usually are:

- search exhaustion (when no more plans to visit)
- cost threshold (when found a plan that cost is lower than a specified cost threshold)
- time (when the search phase is running for too long)

### Volcano query planner

Volcano query planner (or Volcano optimizer generator) is a cost-based query planner

Volcano planner uses dynamic programming approach to find the best query plan from the list of enumerated plans.

details: https://ieeexplore.ieee.org/document/344061 (I'm too lazy to explain the paper here)

Here is a great explanation: https://15721.courses.cs.cmu.edu/spring2017/slides/15-optimizer2.pdf#page=9

## Drafting our cost-based query planner

Our query planner, is a cost based query planner, following the basic idea of Volcano query planner
Our planner will be consisted of 2 main phases:

- exploration/search phase
- implementation/optimization phase

```mermaid
graph LR
    ast((AST))
    logical_plan[Plan]
    explored_plans["`
        Plan #1
        ...
        Plan #N
    `"]
    implementation_plan["Plan #X (best plan)"]
    ast -- convert to logical plan --> logical_plan
    logical_plan -- exploration phase --> explored_plans
    explored_plans -- optimization phase --> implementation_plan
    linkStyle 1,2 color: orange, stroke: orange, stroke-width: 5px
```

#### Glossary

##### Logical plan

Logical plan is the datastructure holding the abstraction of transformation step required to execute the query.

Here is an example of a logical plan:

```mermaid
graph TD
    1["PROJECT tbl1.id, tbl1.field1, tbl2.field1, tbl2.field2, tbl3.id, tbl3.field2, tbl3.field2"];
    2["JOIN"];
    3["SCAN tbl1"];
    4["JOIN"];
    5["SCAN tbl2"];
    6["SCAN tbl3"];
    1 --> 2;
    2 --> 3;
    2 --> 4;
    4 --> 5;
    4 --> 6;
```

##### Physical plan

While logical plan only holds the abstraction, physical plan is the datastructure holding the implementation details.
Each logical plan will have multiple physical plans. For example, a logical JOIN might has many physical plans such as
HASH JOIN, MERGE JOIN, BROADCAST JOIN, etc.

##### Equivalent Group

Equivalent group is a group of equivalent expressions (which for each expression, their logical plan is logically
equivalent)

e.g.

```mermaid
graph TD
    subgraph Group#8
        Expr#8["SCAN tbl2 (field1, field2, id)"]
    end
    subgraph Group#2
        Expr#2["SCAN tbl2"]
    end
    subgraph Group#11
        Expr#11["JOIN"]
    end
    Expr#11 --> Group#7
    Expr#11 --> Group#10
    subgraph Group#5
        Expr#5["JOIN"]
    end
    Expr#5 --> Group#1
    Expr#5 --> Group#4
    subgraph Group#4
        Expr#4["JOIN"]
    end
    Expr#4 --> Group#2
    Expr#4 --> Group#3
    subgraph Group#7
        Expr#7["SCAN tbl1 (id, field1)"]
    end
    subgraph Group#1
        Expr#1["SCAN tbl1"]
    end
    subgraph Group#10
        Expr#10["JOIN"]
    end
    Expr#10 --> Group#8
    Expr#10 --> Group#9
    subgraph Group#9
        Expr#9["SCAN tbl3 (id, field2)"]
    end
    subgraph Group#3
        Expr#3["SCAN tbl3"]
    end
    subgraph Group#6
        Expr#12["PROJECT tbl1.id, tbl1.field1, tbl2.field1, tbl2.field2, tbl3.id, tbl3.field2, tbl3.field2"]
        Expr#6["PROJECT tbl1.id, tbl1.field1, tbl2.field1, tbl2.field2, tbl3.id, tbl3.field2, tbl3.field2"]
    end
    Expr#12 --> Group#11
    Expr#6 --> Group#5
```

Here we can see `Group#6` is having 2 equivalent expressions, which are both representing the same query (one is doing
scan from table then project, one is pushing down the projection down to SCAN node).

##### Transformation rule

Transformation rule is the rule to transform from one logical plan to another logical equivalent logical plan

For example, the plan:

```mermaid
graph TD
    1["PROJECT tbl1.id, tbl1.field1, tbl2.field1, tbl2.field2, tbl3.id, tbl3.field2, tbl3.field2"];
    2["JOIN"];
    3["SCAN tbl1"];
    4["JOIN"];
    5["SCAN tbl2"];
    6["SCAN tbl3"];
    1 --> 2;
    2 --> 3;
    2 --> 4;
    4 --> 5;
    4 --> 6;
```

when apply the projection pushdown transformation, is transformed to:

```mermaid
graph TD
    1["PROJECT *.*"];
    2["JOIN"];
    3["SCAN tbl1 (id, field1)"];
    4["JOIN"];
    5["SCAN tbl2 (field1, field2)"];
    6["SCAN tbl3 (id, field2, field2)"];
    1 --> 2;
    2 --> 3;
    2 --> 4;
    4 --> 5;
    4 --> 6;
```

The transformation rule can be affect by logical traits/properties such as table schema, data statistics, etc.

##### Implementation rule

Implementation rule is the rule to return the physical plans given logical plan.

The implementation rule can be affect by physical traits/properties such as data layout (sorted or not), etc.

#### Exploration phase

In the exploration phase, the planner will apply transformation rules, generating equivalent logical plans

For example, the plan:

```mermaid
graph TD
    1326583549["PROJECT tbl1.id, tbl1.field1, tbl2.id, tbl2.field1, tbl2.field2, tbl3.id, tbl3.field2, tbl3.field2"];
    -425111028["JOIN"];
    -349388609["SCAN tbl1"];
    1343755644["JOIN"];
    -1043437086["SCAN tbl2"];
    -1402686787["SCAN tbl3"];
    1326583549 --> -425111028;
    -425111028 --> -349388609;
    -425111028 --> 1343755644;
    1343755644 --> -1043437086;
    1343755644 --> -1402686787;
```

After applying transformation rules, result in the following graph:

```mermaid
graph TD
    subgraph Group#8
        Expr#8["SCAN tbl2 (id, field1, field2)"]
    end
    subgraph Group#11
        Expr#11["JOIN"]
        Expr#14["JOIN"]
    end
    Expr#11 --> Group#7
    Expr#11 --> Group#10
    Expr#14 --> Group#8
    Expr#14 --> Group#12
    subgraph Group#2
        Expr#2["SCAN tbl2"]
    end
    subgraph Group#5
        Expr#5["JOIN"]
        Expr#16["JOIN"]
    end
    Expr#5 --> Group#1
    Expr#5 --> Group#4
    Expr#16 --> Group#2
    Expr#16 --> Group#13
    subgraph Group#4
        Expr#4["JOIN"]
    end
    Expr#4 --> Group#2
    Expr#4 --> Group#3
    subgraph Group#13
        Expr#15["JOIN"]
    end
    Expr#15 --> Group#1
    Expr#15 --> Group#3
    subgraph Group#7
        Expr#7["SCAN tbl1 (id, field1)"]
    end
    subgraph Group#1
        Expr#1["SCAN tbl1"]
    end
    subgraph Group#10
        Expr#10["JOIN"]
    end
    Expr#10 --> Group#8
    Expr#10 --> Group#9
    subgraph Group#9
        Expr#9["SCAN tbl3 (id, field2)"]
    end
    subgraph Group#3
        Expr#3["SCAN tbl3"]
    end
    subgraph Group#12
        Expr#13["JOIN"]
    end
    Expr#13 --> Group#7
    Expr#13 --> Group#9
    subgraph Group#6
        Expr#12["PROJECT tbl1.id, tbl1.field1, tbl2.id, tbl2.field1, tbl2.field2, tbl3.id, tbl3.field2, tbl3.field2"]
        Expr#6["PROJECT tbl1.id, tbl1.field1, tbl2.id, tbl2.field1, tbl2.field2, tbl3.id, tbl3.field2, tbl3.field2"]
    end
    Expr#12 --> Group#11
    Expr#6 --> Group#5
```

Here we can see that projection pushdown rule and join reorder rule are applied.

#### Optimization phase

The optimization phase, is to traverse the expanded tree in exploration phase, to find the best
plan for our query.

This "actually" is tree search optimization, so you can use any tree search algorithm you can imagine (but you have to
make sure it's correct).

Here is the example of generated physical plan after optimization phase:

```mermaid

graph TD
    Group#6["
    Group #6
Selected: PROJECT tbl1.id, tbl1.field1, tbl2.id, tbl2.field1, tbl2.field2, tbl3.id, tbl3.field2, tbl3.field2
Operator: ProjectOperator
Cost: Cost(cpu=641400.00, mem=1020400012.00, time=1000000.00)
"]
Group#6 --> Group#11
Group#11["
Group #11
Selected: JOIN
Operator: HashJoinOperator
Cost: Cost(cpu=641400.00, mem=1020400012.00, time=1000000.00)
"]
Group#11 --> Group#7
Group#11 --> Group#10
Group#7["
Group #7
Selected: SCAN tbl1 (id, field1)
Operator: NormalScanOperator
Cost: Cost(cpu=400.00, mem=400000.00, time=1000.00)
"]
Group#10["
Group #10
Selected: JOIN
Operator: MergeJoinOperator
Traits: SORTED
Cost: Cost(cpu=640000.00, mem=20000012.00, time=1100000.00)
"]
Group#10 --> Group#8
Group#10 --> Group#9
Group#8["
Group #8
Selected: SCAN tbl2 (id, field1, field2)
Operator: NormalScanOperator
Traits: SORTED
Cost: Cost(cpu=600000.00, mem=12.00, time=1000000.00)
"]
Group#9["
Group #9
Selected: SCAN tbl3 (id, field2)
Operator: NormalScanOperator
Traits: SORTED
Cost: Cost(cpu=40000.00, mem=20000000.00, time=100000.00)
"]
```

The generated plan has shown the selected logical plan, the estimated cost, and the physical operator

#### Optimize/search termination

Our planner will perform exhaustion search to find the best plan

## Diving into the codes

Since the code of the planner is big, so I will not write step-by-step guide, but I will explain every piece of the code
instead

### The query language

Here we will define a query language which used thoroughly this tutorial

```sql
SELECT emp.id,
       emp.code,
       dept.dept_name,
       emp_info.name,
       emp_info.origin
FROM emp
         JOIN dept ON emp.id = dept.emp_id
         JOIN emp_info ON dept.emp_id = emp_info.id
```

The query language we will implement is a SQL-like language.
However, for the sake of simplicity, we will restrict its functionality and syntax.

The language is appeared in form of

```sql
SELECT tbl.field, [...]
FROM tbl JOIN [...]
```

It will only support for `SELECT` and `JOIN`, also the field in Select statement must be fully qualified (in form
of `table.field`), all other functionalities will not be supported

#### The AST

First, we have to define the AST for our language. AST (
or [Abstract Syntax Tree](https://en.wikipedia.org/wiki/Abstract_syntax_tree)) is a tree used to represent the syntactic
structure of a text.

Since our language is so simple, we just can define the AST structure in several line of codes:

```scala
sealed trait Identifier

case class TableID(id: String) extends Identifier

case class FieldID(table: TableID, id: String) extends Identifier

sealed trait Statement

case class Table(table: TableID) extends Statement

case class Join(left: Statement, right: Statement, on: Seq[(FieldID, FieldID)]) extends Statement

case class Select(fields: Seq[FieldID], from: Statement) extends Statement

```

For example, a query

```sql
SELECT tbl1.id,
       tbl1.field1,
       tbl2.id,
       tbl2.field1,
       tbl2.field2,
       tbl3.id,
       tbl3.field2,
       tbl3.field2
FROM tbl1
         JOIN tbl2 ON tbl1.id = tbl2.id
         JOIN tbl3 ON tbl2.id = tbl3.id
```

can be represented as

```scala
Select(
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
        FieldID(TableID("tbl2"), "id") -> FieldID(TableID("tbl3"), "id")
      )
    ),
    Seq(
      FieldID(TableID("tbl1"), "id") -> FieldID(TableID("tbl2"), "id")
    )
  )
)
```

#### A simple query parser

After defined the AST structure, we will have to write the query parser, which is used to convert the text query into
AST form.

Since this guide is using Scala for implementation, we will
choose [scala-parser-combinators](https://github.com/scala/scala-parser-combinators) to create our query parser.

Query parser class:

```scala
object QueryParser extends ParserWithCtx[QueryExecutionContext, Statement] with RegexParsers {

  override def parse(in: String)(implicit ctx: QueryExecutionContext): Either[Throwable, Statement] = {
    Try(parseAll(statement, in) match {
      case Success(result, _) => Right(result)
      case NoSuccess(msg, _) => Left(new Exception(msg))
    }) match {
      case util.Failure(ex) => Left(ex)
      case util.Success(value) => value
    }
  }

  private def select: Parser[Select] = ??? // we will implement it in later section

  private def statement: Parser[Statement] = select
}

```

Then define some parse rules:

```scala
// common
private def str: Parser[String] = """[a-zA-Z0-9_]+""".r
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
```

Here are two rules, which are used to parse the identifiers: `TableID` and `FieldID`.

Table ID (or table name) usually only contains characters, numbers and underscores (`_`), so we will use a simple
regex `[a-zA-Z0-9_]+` to identify the table name.

On the other hand, Field ID (for field qualifier) in our language is fully-qualified-field-name. Normally it's in form
of `table.field`, and field name also usually only contains characters, numbers and underscores, so we will use the
regex `[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+` to parser the field name.

After defining the rules for parsing the identifiers, we can now define rules to parse query statement:

```scala
// statement
private def table: Parser[Table] = tableId ^^ (t => Table(t))
private def subQuery: Parser[Statement] = "(" ~> select <~ ")"
```

The `table` rule is a simple rule, it just creates `Table` node by using the parsed `TableID` from `tableId` rule.

The `subQuery`, is the rule to parse the sub-query. In SQL, we can write a query which is looked like this:

```sql
SELECT a
FROM (SELECT b FROM c) d
```

The `SELECT b FROM c` is the sub-query in above statement. Here, in our simple query language, we will indicate a
statement is a sub-query if it is enclosed by a pair of parentheses (`()`). Since our language only have SELECT
statement, we can write the parse rule as following:

```scala
def subQuery: Parser[Statement] = "(" ~> select <~ ")"
```

Now we will define the parse rules for SELECT statement:

```scala
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
          val joinOn = join._2.map(on => on._1._1 -> on._2)
          statement -> joinOn
        }
        chain(src, temp)
      } else {
        src
      }
      Select(fields, p)
  }
```

In SQL, we can use a sub-query as a JOIN source. For example:

```sql
SELECT *.*
FROM tbl1
    JOIN (SELECT *.* FROM tbl2)
    JOIN tbl3
```

So our parser will also implement rules to parse the sub-query in the JOIN part of the statement, that's why we have the
parse rule:

```scala
"SELECT" ~ rep1sep(fieldId, ",") ~ "FROM" ~ fromSource ~ rep("JOIN" ~ fromSource ~ "ON" ~ rep1(fieldId ~ "=" ~ fieldId)
```

See [QueryParser.scala](core%2Fsrc%2Fmain%2Fscala%2Fcore%2Fql%2FQueryParser.scala) for full implementation

#### Testing our query parser

See [QueryParserSpec.scala](core%2Fsrc%2Ftest%2Fscala%2Fcore%2Fql%2FQueryParserSpec.scala)

### Logical plan

After generate the AST from the text query, we can directly convert it to the logical plan

First, lets define the interface for our logical plan:

```scala
sealed trait LogicalPlan {
  def children(): Seq[LogicalPlan]
}

```

`children` is the list of child logical plan. For example:

```mermaid
graph TD
    1326583549["PROJECT tbl1.id, tbl1.field1, tbl2.id, tbl2.field1, tbl2.field2, tbl3.id, tbl3.field2, tbl3.field2"];
    -425111028["JOIN"];
    -349388609["SCAN tbl1"];
    1343755644["JOIN"];
    -1043437086["SCAN tbl2"];
    -1402686787["SCAN tbl3"];
    1326583549 --> -425111028;
    -425111028 --> -349388609;
    -425111028 --> 1343755644;
    1343755644 --> -1043437086;
    1343755644 --> -1402686787;
```

The child nodes of the `PROJECT` node is the first `JOIN` node. The first `JOIN` node has 2 children, which are the
second `JOIN` node and `SCAN tbl1` node. So on, ...

Since our query language is simple, we only need 3 types of logical node:

- PROJECT: represent the projection operator in relation algebra
- JOIN: represent the logical join
- SCAN: represent the table scan

```scala
case class Scan(table: ql.TableID, projection: Seq[String]) extends LogicalPlan {
  override def children(): Seq[LogicalPlan] = Seq.empty
}

case class Project(fields: Seq[ql.FieldID], child: LogicalPlan) extends LogicalPlan {
  override def children(): Seq[LogicalPlan] = Seq(child)
}

case class Join(left: LogicalPlan, right: LogicalPlan, on: Seq[(ql.FieldID, ql.FieldID)]) extends LogicalPlan {
  override def children(): Seq[LogicalPlan] = Seq(left, right)
}

```

Then we can write the function to convert the AST into logical plan:

```scala
def toPlan(node: ql.Statement): LogicalPlan = {
  node match {
    case ql.Table(table) => Scan(table, Seq.empty)
    case ql.Join(left, right, on) => Join(toPlan(left), toPlan(right), on)
    case ql.Select(fields, from) => Project(fields, toPlan(from))
  }
}
```

See [LogicalPlan.scala](core%2Fsrc%2Fmain%2Fscala%2Fcore%2Fplanner%2Fvolcano%2Flogicalplan%2FLogicalPlan.scala) for full
implementation

### The equivalent groups

#### Group

We can define classes for Group as following:

```scala
case class Group(
                  id: Long,
                  equivalents: mutable.HashSet[GroupExpression]
                ) {
  val explorationMark: ExplorationMark = new ExplorationMark
  var implementation: Option[GroupImplementation] = None
}

case class GroupExpression(
                            id: Long,
                            plan: LogicalPlan,
                            children: mutable.MutableList[Group]
                          ) {
  val explorationMark: ExplorationMark = new ExplorationMark
  val appliedTransformations: mutable.HashSet[TransformationRule] = mutable.HashSet()
}

```

`Group` is the set of plans which are logically equivalent.

Each `GroupExpression` represents a logical plan node. Since we've defined a logical plan node will have a list of child
nodes (in the previous section), and the `GroupExpression` represents a logical plan node, and the `Group` represents a
set of equivalent plans, so the children of `GroupExpression` is a list of `Group`

e.g.

```mermaid
graph TD
    subgraph Group#8
        Expr#8
    end
    subgraph Group#2
        Expr#2
    end
    subgraph Group#11
        Expr#11
    end
    Expr#11 --> Group#7
    Expr#11 --> Group#10
    subgraph Group#5
        Expr#5
    end
    Expr#5 --> Group#1
    Expr#5 --> Group#4
    subgraph Group#4
        Expr#4
    end
    Expr#4 --> Group#2
    Expr#4 --> Group#3
    subgraph Group#7
        Expr#7
    end
    subgraph Group#1
        Expr#1
    end
    subgraph Group#10
        Expr#10
    end
    Expr#10 --> Group#8
    Expr#10 --> Group#9
    subgraph Group#9
        Expr#9
    end
    subgraph Group#3
        Expr#3
    end
    subgraph Group#6
        Expr#12
        Expr#6
    end
    Expr#12 --> Group#11
    Expr#6 --> Group#5
```

As we can see here, the `Group#6` has 2 equivalent expressions: `Expr#12` and `Expr#6`, and the children of `Expr#12`
is `Group#11`

#### Memo

Memo is a bunch of helpers to help constructing the equivalent groups. Memo is consists of several hashmap to cache the
group and group expression and methods to register new group or group expression.

```scala
class Memo(
            groupIdGenerator: Generator[Long] = new LongGenerator,
            groupExpressionIdGenerator: Generator[Long] = new LongGenerator
          ) {
  val groups: mutable.HashMap[Long, Group] = mutable.HashMap[Long, Group]()
  val parents: mutable.HashMap[Long, Group] = mutable.HashMap[Long, Group]() // lookup group from group expression ID
  val groupExpressions: mutable.HashMap[LogicalPlan, GroupExpression] = mutable.HashMap[LogicalPlan, GroupExpression]()

  def getOrCreateGroupExpression(plan: LogicalPlan): GroupExpression = {
    val children = plan.children()
    val childGroups = children.map(child => getOrCreateGroup(child))
    groupExpressions.get(plan) match {
      case Some(found) => found
      case None =>
        val id = groupExpressionIdGenerator.generate()
        val children = mutable.MutableList() ++ childGroups
        val expression = GroupExpression(
          id = id,
          plan = plan,
          children = children
        )
        groupExpressions += plan -> expression
        expression
    }
  }

  def getOrCreateGroup(plan: LogicalPlan): Group = {
    val exprGroup = getOrCreateGroupExpression(plan)
    val group = parents.get(exprGroup.id) match {
      case Some(group) =>
        group.equivalents += exprGroup
        group
      case None =>
        val id = groupIdGenerator.generate()
        val equivalents = mutable.HashSet() + exprGroup
        val group = Group(
          id = id,
          equivalents = equivalents
        )
        groups.put(id, group)
        group
    }
    parents += exprGroup.id -> group
    group
  }
}

```

See [Memo.scala](core%2Fsrc%2Fmain%2Fscala%2Fcore%2Fplanner%2Fvolcano%2Fmemo%2FMemo.scala) for full implementation

### Initialization

#### The root group

### Exploration phase

#### Transformation rule

#### Plan enumerations

#### Visualize our transformations

### Optimization phase

#### More plan enumerations

#### Estimating the cost

#### Finding the best plan

#### Visualize our best plan

### Bonus: query execution

#### Another Volcano

#### The operators

#### Testing a simple query

# Thanks

:beers: