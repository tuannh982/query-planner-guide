package core.planner.memo

import core.planner.volcano.logicalplan.{Join, Project, Scan}
import core.planner.volcano.memo.{Group, GroupExpression, Memo}
import core.ql
import core.ql.FieldID
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable

class MemoSpec extends AnyFlatSpec with MockFactory {
  behavior of "Memo"

  it should "correctly init root group (1)" in {
    val plan = Project(
      Seq(
        FieldID(ql.TableID("tbl1"), "id"),
        FieldID(ql.TableID("tbl1"), "field1"),
        FieldID(ql.TableID("tbl2"), "id"),
        FieldID(ql.TableID("tbl2"), "field1"),
        FieldID(ql.TableID("tbl2"), "field2"),
        FieldID(ql.TableID("tbl3"), "id"),
        FieldID(ql.TableID("tbl3"), "field2"),
        FieldID(ql.TableID("tbl3"), "field2")
      ),
      Join(
        Scan(ql.TableID("tbl1"), Seq.empty),
        Join(
          Scan(ql.TableID("tbl2"), Seq.empty),
          Scan(ql.TableID("tbl3"), Seq.empty)
        )
      )
    )
    val memo  = new Memo
    val group = memo.getOrCreateGroup(plan)
    assert(
      group == Group(
        0,
        mutable.HashSet(
          GroupExpression(
            0,
            plan,
            mutable.MutableList(
              Group(
                0,
                mutable.HashSet(
                  GroupExpression(
                    0,
                    Join(
                      Scan(ql.TableID("tbl1"), Seq.empty),
                      Join(
                        Scan(ql.TableID("tbl2"), Seq.empty),
                        Scan(ql.TableID("tbl3"), Seq.empty)
                      )
                    ),
                    mutable.MutableList(
                      Group(
                        0,
                        mutable.HashSet(
                          GroupExpression(0, Scan(ql.TableID("tbl1"), Seq.empty), mutable.MutableList[Group]())
                        )
                      ),
                      Group(
                        0,
                        mutable.HashSet(
                          GroupExpression(
                            0,
                            Join(
                              Scan(ql.TableID("tbl2"), Seq.empty),
                              Scan(ql.TableID("tbl3"), Seq.empty)
                            ),
                            mutable.MutableList(
                              Group(
                                0,
                                mutable.HashSet(
                                  GroupExpression(0, Scan(ql.TableID("tbl2"), Seq.empty), mutable.MutableList[Group]())
                                )
                              ),
                              Group(
                                0,
                                mutable.HashSet(
                                  GroupExpression(0, Scan(ql.TableID("tbl3"), Seq.empty), mutable.MutableList[Group]())
                                )
                              )
                            )
                          )
                        )
                      )
                    )
                  )
                )
              )
            )
          )
        )
      )
    )
  }
}
