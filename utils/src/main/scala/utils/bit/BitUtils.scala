package utils.bit

object BitUtils {

  def getBit(value: Long, bit: Int): Boolean = {
    (value & (1L << bit)) != 0
  }

  def setBit(value: Long, bit: Int, on: Boolean): Long = {
    if (on) {
      value | 1L << bit
    } else {
      value & ~(1L << bit)
    }
  }
}
