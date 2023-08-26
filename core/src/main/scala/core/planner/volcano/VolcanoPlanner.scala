package core.planner.volcano

import core.ctx.QueryExecutionContext
import core.execution.Operator
import core.planner.Planner
import core.planner.volcano.logicalplan.LogicalPlan
import core.planner.volcano.memo.Group
import core.planner.volcano.rules.TransformationRule
import core.planner.volcano.rules.transform.ProjectionPushDown
import core.ql.Statement

class VolcanoPlanner extends Planner {

  // round
  private val initialRound = 0

  // multi-stage transformation
  private val transformationRules: Seq[Seq[TransformationRule]] = Seq(
    Seq(new ProjectionPushDown)
  )

  override def getPlan(expr: Statement)(implicit ctx: QueryExecutionContext): Operator = {
    implicit val plannerCtx: VolcanoPlannerContext = new VolcanoPlannerContext(ctx)
    initialize(expr)
    explore()
    ???
  }

  // initialization phase
  def initialize(query: Statement)(implicit ctx: VolcanoPlannerContext): Unit = {
    ctx.query = query
    ctx.rootPlan = LogicalPlan.toPlan(ctx.query)
    ctx.rootGroup = ctx.memo.getOrCreateGroup(ctx.rootPlan)
    // assuming this is first the exploration round,
    // by marking the initialRound(0) as explored,
    // it will be easier to visualize the different between rounds (added nodes, add connections)
    ctx.memo.groups.values.foreach(_.explorationMark.markExplored(initialRound))
    ctx.memo.groupExpressions.values.foreach(_.explorationMark.markExplored(initialRound))
  }

  // exploration phase
  def explore()(implicit ctx: VolcanoPlannerContext): Unit = {
    for (r <- transformationRules.indices) {
      exploreGroup(ctx.rootGroup, transformationRules(r), r + 1)
    }
  }

  //noinspection DuplicatedCode
  private def exploreGroup(group: Group, rules: Seq[TransformationRule], round: Int)(
    implicit ctx: VolcanoPlannerContext
  ): Unit = {
    while (!group.explorationMark.isExplored(round)) {
      group.explorationMark.markExplored(round)
      // explore all child groups
      group.equivalents.foreach { equivalent =>
        if (!equivalent.explorationMark.isExplored(round)) {
          equivalent.explorationMark.markExplored(round)
          equivalent.children.foreach { child =>
            exploreGroup(child, rules, round)
            if (equivalent.explorationMark.isExplored(round) && child.explorationMark.isExplored(round)) {
              equivalent.explorationMark.markExplored(round)
            } else {
              equivalent.explorationMark.markUnexplored(round)
            }
          }
        }
        // fire transformation rules to explore all the possible transformations
        rules.foreach { rule =>
          if (!equivalent.appliedTransformations.contains(rule) && rule.`match`(equivalent)) {
            val transformed = rule.transform(equivalent)
            if (!group.equivalents.contains(transformed)) {
              group.equivalents += transformed
              transformed.explorationMark.markUnexplored(round)
              group.explorationMark.markUnexplored(round)
            }
          }
        }
        if (group.explorationMark.isExplored(round) && equivalent.explorationMark.isExplored(round)) {
          group.explorationMark.markExplored(round)
        } else {
          group.explorationMark.markUnexplored(round)
        }
      }
    }
  }
}
