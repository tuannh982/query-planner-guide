package core.planner.volcano

import core.execution.Operator
import core.planner.Planner
import core.planner.volcano.logicalplan.LogicalPlan
import core.planner.volcano.memo.{Group, GroupImplementation}
import core.planner.volcano.rules.{ImplementationRule, TransformationRule}
import core.ql.Statement

class VolcanoPlanner extends Planner[VolcanoPlannerContext] {

  // round
  private val initialRound = 0
  // multi-stage transformation
  private val transformationRules: Seq[Seq[TransformationRule]] = TransformationRule.ruleBatches
  // combined implementation rule, from all implementation rules
  private val combinedImplementationRule: ImplementationRule = ImplementationRule.combined

  override def getPlan(expr: Statement)(implicit ctx: VolcanoPlannerContext): Operator = {
    initialize(expr)
    explore()
    implement()
    ctx.rootGroup.implementation match {
      case Some(implementation) => implementation.physicalPlan.operator()
      case None                 => throw new Exception("No implementation found, something went wrong!")
    }
  }

  /**
    * Initialization phase
    */

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

  /**
    * Exploration phase
    */

  def explore()(implicit ctx: VolcanoPlannerContext): Unit = {
    for (r <- transformationRules.indices) {
      exploreGroup(ctx.rootGroup, transformationRules(r), r + 1)
    }
  }

  //noinspection DuplicatedCode
  private def exploreGroup(
    group: Group,
    rules: Seq[TransformationRule],
    round: Int
  )(implicit ctx: VolcanoPlannerContext): Unit = {
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

  /**
    * Implementation phase
    */
  def implement()(implicit ctx: VolcanoPlannerContext): Unit = {
    ctx.rootGroup.implementation = Option(implementGroup(ctx.rootGroup, combinedImplementationRule))
  }

  private def implementGroup(group: Group, combinedRule: ImplementationRule)(
    implicit ctx: VolcanoPlannerContext
  ): GroupImplementation = {
    group.implementation match {
      case Some(implementation) => implementation
      case None =>
        var bestImplementation = Option.empty[GroupImplementation]
        group.equivalents.foreach { equivalent =>
          val physicalPlanBuilders = combinedRule.physicalPlanBuilders(equivalent)
          val childPhysicalPlans = equivalent.children.map { child =>
            val childImplementation = implementGroup(child, combinedRule)
            child.implementation = Option(childImplementation)
            childImplementation.physicalPlan
          }
          // calculate the implementation, and update the best cost for group
          physicalPlanBuilders.foreach { builder =>
            val physicalPlan = builder.build(childPhysicalPlans)
            val cost         = physicalPlan.cost()
            bestImplementation match {
              case Some(currentBest) =>
                if (ctx.costModel.isBetter(currentBest.cost, cost)) {
                  bestImplementation = Option(
                    GroupImplementation(
                      physicalPlan = physicalPlan,
                      cost = cost,
                      selectedEquivalentExpression = equivalent
                    )
                  )
                }
              case None =>
                bestImplementation = Option(
                  GroupImplementation(
                    physicalPlan = physicalPlan,
                    cost = cost,
                    selectedEquivalentExpression = equivalent
                  )
                )
            }
          }
        }
        bestImplementation.get
    }
  }
}
