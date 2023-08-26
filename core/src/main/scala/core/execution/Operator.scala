package core.execution

trait Operator {
  def next(): Any
  def `type`(): Class[_]
}

class MockOperator extends Operator {
  override def next(): Any          = ???
  override def `type`(): Class[Any] = ???
}

case class ProjectOperator(child: Operator)                               extends MockOperator
case class NormalScanOperator(tableName: String, projection: Seq[String]) extends MockOperator
case class MergeJoinOperator(leftChild: Operator, rightChild: Operator)   extends MockOperator
case class HashJoinOperator(leftChild: Operator, rightChild: Operator)    extends MockOperator
