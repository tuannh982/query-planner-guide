package core.utils.visualization

import core.planner.volcano.logicalplan.LogicalPlan

object LogicalPlanVizUtils {

  implicit class Mermaid(plan: LogicalPlan) {

    private def getEdges(node: LogicalPlan): Seq[(LogicalPlan, LogicalPlan)] = {
      val children   = node.children()
      val edges      = children.map(child => node -> child)
      val childEdges = children.flatMap(getEdges)
      edges ++ childEdges
    }

    def mermaidViz: String = {
      val sb    = new StringBuilder()
      val edges = getEdges(plan)
      val nodes = edges.flatMap { case (from, to) => Seq(from, to) }.distinct
      sb.append("graph TD\n")
      nodes.foreach { node =>
        sb.append(s"""  ${node.hashCode()}["${node.describe()}"];\n""")
      }
      edges.foreach {
        case (from, to) =>
          sb.append(s"  ${from.hashCode()} --> ${to.hashCode()};\n")
      }
      sb.toString()
    }
  }
}
