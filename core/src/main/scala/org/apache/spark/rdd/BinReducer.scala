package org.apache.spark.rdd

import java.io.File
import java.io.FilenameFilter
import java.io.IOException
import java.io.PrintWriter
import java.util.StringTokenizer

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.reflect.ClassTag

import org.apache.spark.{Partition, SparkEnv, TaskContext}
import org.apache.spark.util.Utils

/**
 * @author liuchang
 */
object BinReducer {
  def getBinReducer[T](command:String) : Iterator[T] => Option[String] = iter => {
    var pb = new ProcessBuilder(PipedRDD.tokenize(command).asJava)
    val proc = pb.start()
    val env = SparkEnv.get
    
    new Thread("stderr reader for " + command) {
      override def run() {
        for (line <- Source.fromInputStream(proc.getErrorStream).getLines) {
          // scalastyle:off println
          System.err.println(line)
          // scalastyle:on println
        }
      }
    }.start()

    
    // Start a thread to feed the process input from our parent's iterator
    new Thread("stdin writer for " + command) {
      override def run() {
        val out = new PrintWriter(proc.getOutputStream)
        for (elem <- iter) {
          out.println(elem)
        }
        out.close()
      }
    }.start()

    val lines = Source.fromInputStream(proc.getInputStream).getLines()

    if(lines.hasNext)
      Some(lines.next())
    else
      None
  }
}