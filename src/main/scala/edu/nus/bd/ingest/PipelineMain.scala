package edu.nus.bd.ingest

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object PipelineMain {

  def main(args: Array[String]): Unit = {
    val filePath = args(0)

    val sparkConf = buildSparkConf()

    implicit val spark = SparkSession
      .builder()
      .config(sparkConf)
      .appName("Boring Pipeline")
      .master("local[*]")
      .getOrCreate()

    LogManager.getRootLogger.setLevel(Level.WARN)
    spark.sparkContext.setLogLevel("ERROR")

    runPipeline(filePath)

  }

  private def runPipeline(filePath: String)(implicit spark: SparkSession) = {

  }


  def buildSparkConf(): SparkConf = new SparkConf()
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .set("spark.sql.streaming.schemaInference", "true")
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")
    .set("spark.scheduler.mode", "FAIR")
    .set("spark.sql.warehouse.dir", "/tmp/awaywarehose")
    .set("spark.kryoserializer.buffer.max", "1g")
}
