package core.planner.volcano.memo

import utils.bit.BitUtils

class ExplorationMark {
  private var bits: Long = 0

  def get: Long                        = bits
  def isExplored(round: Int): Boolean  = BitUtils.getBit(bits, round)
  def markExplored(round: Int): Unit   = bits = BitUtils.setBit(bits, round, on = true)
  def markUnexplored(round: Int): Unit = bits = BitUtils.setBit(bits, round, on = false)
}
