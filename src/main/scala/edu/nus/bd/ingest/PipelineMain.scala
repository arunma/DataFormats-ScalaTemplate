package edu.nus.bd.ingest

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

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

    val schema = StructType(Seq(
      StructField("url", StringType),
      StructField("urlid", IntegerType),
      StructField("alchemy_category", StringType),
      StructField("is_news", StringType),
      StructField("lengthyLinkDomain", IntegerType),
      StructField("news_front_page", StringType),
      StructField("non_markup_alphanum_characters", LongType),
      StructField("numberOfLinks", IntegerType),
      StructField("numwords_in_url", IntegerType),
      StructField("spelling_errors_ratio", DoubleType)
    ))

    val sourceRawDf =
      spark
        .read
        .format("csv")
        .option("delimiter", "\t")
        .schema(schema)
        .load(filePath)

    sourceRawDf.printSchema()
    sourceRawDf.show()

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
