package core.planner.volcano.rules

import core.planner.volcano.VolcanoPlannerContext
import core.planner.volcano.logicalplan.{Join, Project, Scan}
import core.planner.volcano.memo.GroupExpression
import core.planner.volcano.physicalplan.PhysicalPlanBuilder

trait ImplementationRule {
  def physicalPlanBuilders(expression: GroupExpression)(implicit ctx: VolcanoPlannerContext): Seq[PhysicalPlanBuilder]
}

object ImplementationRule {

  def combined: ImplementationRule = new ImplementationRule {

    override def physicalPlanBuilders(
      expression: GroupExpression
    )(implicit ctx: VolcanoPlannerContext): Seq[PhysicalPlanBuilder] = {
      expression.plan match {
        case node @ Scan(_, _)    => implement.Scan(node)
        case node @ Project(_, _) => implement.Project(node)
        case node @ Join(_, _)    => implement.Join(node)
      }
    }
  }
}
