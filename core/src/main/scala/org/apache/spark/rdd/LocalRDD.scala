package org.apache.spark.rdd

import java.text.SimpleDateFormat
import java.util.Date
import java.io.EOFException

import scala.collection.immutable.Map
import scala.reflect.ClassTag
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.Reporter
import org.apache.hadoop.mapred.JobID
import org.apache.hadoop.mapred.TaskAttemptID
import org.apache.hadoop.mapred.TaskID
import org.apache.hadoop.mapred.lib.CombineFileSplit
import org.apache.hadoop.util.ReflectionUtils

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.rdd.HadoopRDD.HadoopMapPartitionsWithSplitRDD
import org.apache.spark.util.{SerializableConfiguration, ShutdownHookManager, NextIterator, Utils}
import org.apache.spark.scheduler.{HostTaskLocation, HDFSCacheTaskLocation}
import org.apache.spark.storage.StorageLevel

/**
 * A Spark split class that wraps around a Hadoop InputSplit.
 */
private[spark] class LocalPartition(rddId: Int, 
    idx: Int,
    _host: String,
    localPath: String)
  extends Partition {

  val host = _host
  
  val path = localPath

  override def hashCode(): Int = 41 * (41 + rddId) + idx

  override val index: Int = idx

  /**
   * Get any environment variables that should be added to the users environment when running pipes
   * @return a Map with the environment variables and corresponding values, it could be empty
   */
  def getPipeEnvVars(): Map[String, String] = {
    val envVars: Map[String, String] = 
      Map("local_file" -> path,
          "map_input_file" -> path,
        "mapreduce_map_input_file" -> path)
    envVars
  }
}

/**
 * @author liuchang
 */
class LocalRDD(
    sc: SparkContext,
    _host: String,
    _path: String
    )
  extends RDD[String](sc, Nil) with Logging {
  val LOCAL_FILE_DIR = "/helio/data/"
  
  val host = _host
  
  val path = _path
  
  override def compute(partition: Partition, context: TaskContext): InterruptibleIterator[String] = {
    val localPartition = partition.asInstanceOf[LocalPartition]
    val itr = scala.io.Source.fromFile(LOCAL_FILE_DIR + localPartition.path).getLines()
    new InterruptibleIterator[String](context, itr)
  }
  
  override def getPreferredLocations(split:Partition): Seq[String] = {
    val localPartition = split.asInstanceOf[LocalPartition]
    Array(localPartition.host)
  }
  
  override def persist(storageLevel: StorageLevel): this.type = {
//    if (storageLevel.deserialized) {
//      logWarning("Caching NewHadoopRDDs as deserialized objects usually leads to undesired" +
//        " behavior because Hadoop's RecordReader reuses the same Writable object for all records." +
//        " Use a map transformation to make copies of the records.")
//    }
    super.persist(storageLevel)
  }
  
  override def getPartitions: Array[Partition] = {
    Array(new LocalPartition(id, 0, _host, _path))
  }
}