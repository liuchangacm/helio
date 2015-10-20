package org.apache.spark.util

import org.apache.spark.Accumulator

/**
 * @author simpl_000
 */
object Helper {
  def add[U, T](map:Map[U, Accumulator[T]], key:U, value:T): Unit = {
    map(key).add(value)
  }
}