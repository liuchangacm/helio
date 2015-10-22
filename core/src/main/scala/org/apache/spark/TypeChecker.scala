package org.apache.spark

import org.apache.spark.rdd.RDD

/**
 * @author simpl_000
 */
trait TypeChecker {
  def check[T, U](rdd:RDD[T], 
      func:(TaskContext, Iterator[T]) => U, 
      resultHandler:(Int, U) => Unit): Boolean
}

object DefaultTypeChecker extends TypeChecker {
  override def check[T, U](rdd:RDD[T], 
      func:(TaskContext, Iterator[T]) => U, 
      resultHandler:(Int, U) => Unit) = true
  
}