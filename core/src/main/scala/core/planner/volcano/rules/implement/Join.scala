package core.planner.volcano.rules.implement

import core.planner.volcano.physicalplan.PhysicalPlanBuilder
import core.planner.volcano.physicalplan.builder.{HashJoinImpl, MergeJoinImpl}
import core.planner.volcano.{VolcanoPlannerContext, logicalplan}

object Join {

  def apply(node: logicalplan.Join)(implicit ctx: VolcanoPlannerContext): Seq[PhysicalPlanBuilder] = {
    val leftFields  = node.on.map(_._1).map(f => s"${f.table.id}.${f.id}")
    val rightFields = node.on.map(_._2).map(f => s"${f.table.id}.${f.id}")
    Seq(
      new HashJoinImpl(leftFields, rightFields),
      new MergeJoinImpl(leftFields, rightFields)
    )
  }
}
