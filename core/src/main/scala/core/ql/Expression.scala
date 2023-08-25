package core.ql

sealed trait Identifier
case class TableID(id: String)                 extends Identifier
case class FieldID(table: TableID, id: String) extends Identifier

sealed trait Statement
case class Table(table: TableID)                         extends Statement
case class Join(left: Statement, right: Statement)       extends Statement
case class Select(fields: Seq[FieldID], from: Statement) extends Statement
