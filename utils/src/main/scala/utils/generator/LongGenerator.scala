package utils.generator

import java.util.concurrent.atomic.AtomicLong

class LongGenerator extends Generator[Long] {
  private val generator = new AtomicLong(0)

  override def generate(): Long = generator.incrementAndGet()
}
