package core.utils.visualization

import core.planner.volcano.memo.{Group, Memo}

import scala.collection.mutable

object MemoVizUtils {

  implicit class Mermaid(memo: Memo) {

    // check if group is traversed after `round` round (including fully explored and partially explored),
    // any future group (group create starting from `round + 1` round) will not be included
    private def exploredGroupAtRound(group: Group, round: Int): Boolean = {
      group.explorationMark.isExplored(round) ||
      group.equivalents.map(_.explorationMark.isExplored(round)).reduce(_ || _)
    }

    // visualize the different in half-interval (`afterRound`, `toRound`]
    // including added nodes & added connections
    // `afterRound` must always smaller than `toRound`
    def showExplorationDiffMermaidViz(afterRound: Int, toRound: Int): String = {
      if (afterRound >= toRound) {
        throw new Exception(s"afterRound($afterRound) must always smaller than toRound($toRound)")
      }
      val newExpressions = new mutable.HashSet[String]() // store group expression id
      val newLinks       = new mutable.HashSet[Int]() // store link index, according to mermaid link styling guide
      var linkIndex      = 0
      val sb             = new StringBuilder()
      sb.append("graph TD\n")
      memo.groups.filter(g => exploredGroupAtRound(g._2, toRound)).foreach { g =>
        val group = g._2
        sb.append(s"""  subgraph Group#${group.id}\n""")
        val equivalents = group.equivalents.filter(_.explorationMark.isExplored(toRound))
        equivalents.foreach { equivalent =>
          sb.append(s"""    Expr#${equivalent.id}["${equivalent.plan.describe()}"]\n""")
        }
        sb.append("  end\n")
        equivalents.foreach { equivalent =>
          equivalent.children.filter(g => exploredGroupAtRound(g, toRound)).foreach { child =>
            sb.append(s"""  Expr#${equivalent.id} --> Group#${child.id}\n""")
            if (afterRound < 0 || !equivalent.explorationMark.isExplored(afterRound)) {
              newLinks += linkIndex
            }
            linkIndex += 1
          }
          if (afterRound < 0 || !equivalent.explorationMark.isExplored(afterRound)) {
            newExpressions += s"Expr#${equivalent.id}"
          }
        }
      }
      newExpressions.foreach { newExpr =>
        sb.append(s"  style $newExpr stroke-width: 4px, stroke: orange\n")
      }
      newLinks.foreach { newLink =>
        sb.append(s"  linkStyle $newLink stroke-width: 4px, stroke: orange\n")
      }
      sb.toString()
    }

    def showBestPhysicalPlanMermaidViz(group: Group): String = {
      val sb = new StringBuilder()
      sb.append("graph TD\n")
      val queue = new mutable.Queue[Group]()
      queue.enqueue(group)
      while (queue.nonEmpty) {
        val group              = queue.dequeue()
        val implementation     = group.implementation.get // all the implementation must be presented
        val selectedExpression = implementation.selectedEquivalentExpression
        val children           = selectedExpression.children
        sb.append(s"""  Group#${group.id}["\n""")
        sb.append(s"    Group #${group.id}\n")
        sb.append(s"      Selected: ${selectedExpression.plan.describe()}\n")
        sb.append(s"      Operator: ${implementation.physicalPlan.operator().getClass.getSimpleName}\n")
        if (implementation.physicalPlan.traits().nonEmpty) {
          sb.append(s"      Traits: ${implementation.physicalPlan.traits().mkString(", ")}\n")
        }
        sb.append(s"      Cost: ${implementation.cost}\n")
        sb.append(s"""  "]\n""")
        children.foreach { child =>
          sb.append(s"""  Group#${group.id} --> Group#${child.id}\n""")
          queue.enqueue(child)
        }
      }
      sb.toString()
    }
  }
}
