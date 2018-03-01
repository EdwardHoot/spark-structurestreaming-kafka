package com.ebay.hadoop.aiops

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.types._

import joptsimple.OptionParser
import org.slf4j.LoggerFactory
import scala.io.Source
import scala.util.parsing.json.JSON

/**
  * Created by pehu on 2017/12/18.
  */
class App {}

object App
  extends java.io.Serializable {

  val LOG = LoggerFactory.getLogger(classOf[App])

  //  def convert(col: Column):Column = {
  //    col.
  //  }

  def main(args: Array[String]): Unit = {

    val optionParser = new OptionParser()
    optionParser.accepts("broker").withRequiredArg()
    //    optionParser.accepts("topics").withRequiredArg()
    //    optionParser.accepts("columns").withRequiredArg()
    optionParser.accepts("fileformat").withRequiredArg()
    optionParser.accepts("newtopics").withRequiredArg()
    val options = optionParser.parse(args: _*)

    //    val valueColumns = options.valueOf("columns").asInstanceOf[String].split(",")
    val newtopics = options.valueOf("newtopics").asInstanceOf[String].split(",")
    val kafkaBrokerStr = options.valueOf("broker").asInstanceOf[String]
    val fileformat = options.valueOf("fileformat").asInstanceOf[String]
    //    val kafkaTopics = options.valuesOf("topics").get(0).toString


    val conf = new SparkConf().setAppName("StructuredStreamingDemo")
      //     .setMaster("spark://master:7077")
      //     .setJars(Array("target/structure-streaming-demo-1.0-SNAPSHOT-jar-with-dependencies.jar"))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.parquet.compression.codec", "uncompressed")


    val spark = SparkSession
      .builder
      .config(conf)
      .appName("StructuredStreamingDemo")
      .getOrCreate()

    import spark.implicits._

    val topicsnum = newtopics.length

    val query = new Array[StreamingQuery](topicsnum)

    for (i <- 0 until topicsnum) {
      println(newtopics(i))
      val inputstream = this.getClass.getResourceAsStream("/metrics_config")
      val str = Source.fromInputStream(inputstream).mkString
      val zan = JSON.parseFull(str).get.asInstanceOf[Map[String, String]]
      val metrics = zan(newtopics(i)).split(",")
      println(metrics)
      println(metrics.length)

      val lines = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBrokerStr)
        .option("subscribe", newtopics(i))
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        //.createOrReplaceGlobalTempView("lines")


      //        val column_num = values.length
      //
      //        val arr = new Array[StructField](column_num+1)
      //        arr(0) = StructField("times", TimestampType, true)
      //        for(i <- 1 until arr.length)
      //        {
      //          arr(i) = StructField(values(i-1), StringType, true)
      //        }
      //
      //        val seq = arr.toSeq


      //    val struct =StructType(
      //
      //      StructField("vmstat_bo", StringType, true) ::
      //      StructField("vmstat_swpd", StringType, true)::
      //      StructField("is_rs", StringType, true) ::
      //      StructField("vmstat_bi", StringType, true)::
      //      StructField("vmstat_free", StringType, true) ::
      //      StructField("vmstat_wa", StringType, true)::
      //      StructField("vmstat_st", StringType, true) ::
      //      StructField("vmstat_us", StringType, true)::
      //      StructField("vmstat_sy", StringType, true) ::
      //      StructField("vmstat_b", StringType, true)::
      //      StructField("hostname", StringType, true) ::
      //      StructField("is_slave", StringType, true)::
      //      StructField("vmstat_si", StringType, true) ::
      //      StructField("vmstat_r", StringType, true)::
      //      StructField("vmstat_so", StringType, true) ::
      //      StructField("is_cli", StringType, true)::
      //        StructField("timestamp", StringType, true) ::
      //        StructField("is_dn", StringType, true)::
      //        StructField("vmstat_cs", StringType, true) ::
      //        StructField("vmstat_id", StringType, true)::
      //        StructField("is_nm", StringType, true) ::
      //        StructField("vmstat_in", StringType, true)::
      //        StructField("vmstat_cache", StringType, true) ::
      //        StructField("clustername", StringType, true)::
      //        StructField("vmstat_buff", StringType, true) ::
      //        StructField("is_master", StringType, true)::
      //        Nil)

      val schema = StructType( metrics.map(v => StructField(v, StringType, true)))
      //        val struct2 = StructType(
      //          StructField("time", TimestampType, true) :: Nil
      //        )

      //    val words = lines
      //      .select(from_json(col("value").cast("string"), schema))
      //      lines.printSchema()
      //      val words = lines.selectExpr("cast(value as string)", "cast(timestamp as timestamp)")
      //        .toDF("value", "times")

//      spark.sql("select cast(value as string) as value, cast(timestamp as timestamp) as receive_timestamp from lines")
//        .select(col("receive_timestamp"), from_json(col("value"), schema) as "json")
//        .createOrReplaceGlobalTempView("lines")
//      val outputDF = spark.sql(
//        """select
//          |year(receive_timestamp) as year,
//          |month(receive_timestamp) as month,
//          |dayofmonth(receive_timestamp) as day,
//          |json.*
//          |from lines
//        """.stripMargin)

      //      words.printSchema()


         var words = lines
                .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").toDF("value","times")
                .select(col("times"),from_json(col("value"), schema) as "json").select("times","json.*")
         words.printSchema()

      //      val words2 = words
      //        //          .select(from_json(col("times"), struct) as "json")
      //        .select(col("times"), from_json(col("value"), struct) as "json")
      //        .select("json.*", "times")
      //        .select(year(col("times")), dayofmonth(col("times")))

      //      words3.printSchema()
      //        五行原本
      //        var words = lines
      //          .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
      //          .select(from_json(col("value"), struct) as "json").select("json.*")
      //       val words2 = lines
      //            .selectExpr("CAST(timestamp AS TIMESTAMP)").toDF("outertime")

      //      words.withColumn("daytime",words.col("timestamp"))
      //         .withWatermark("timestamp", "10 minutes")
      //    val words = lines
      //      .groupBy(window($"timestamp", "10 minutes", "5 minutes"), $"value")

      //      .selectExpr("CAST(value AS STRING)")
      //      .as[(String)]

      //    val windowedCounts = words
      //      .withWatermark("timestamp", "10 minutes")
      //      .groupBy(
      //        window($"timestamp", "10 minutes", "5 minutes"),
      //        $"value")
      //      .count()
      //    windowedCounts.printSchema()
      //    windowedCounts.createGlobalTempView("wc")
      //    val records2 = spark.sql("SELECT * from wc")
      //    val query2 = records2.writeStream.format("csv")
      //     .option("path","/target/spark015").option("checkpointLocation","/target/sparkcheck").start()

      //words.printSchema()
      //words2.printSchema()
      //words.withColumn("time",lines.col("timestamp"))
      //      五行原本
      //        val words3 = words.withColumnRenamed("timestamp","innertime")
      //        words3.printSchema()
      //        words2.printSchema()
      //        val words4 = words3.withColumn("times",words2("outertime"))
      //        words4.printSchema()

      //    words.createOrReplaceTempView("df")
      //    val records = spark.sql("SELECT value from df")

      query(i) = words
        .withColumn("year",year($"times"))
        .withColumn("month",month($"times")).withColumn("day",dayofmonth($"times"))
        .writeStream.format(fileformat)
        .option("path", "/structure2/" + newtopics(i))
        .partitionBy("year", "month", "day")
        .option("checkpointLocation", "/structure2/" + newtopics(i) + "/sparkcheck")
        .trigger(ProcessingTime("600 seconds")).start()


    }
    for (i <- 0 until topicsnum) {
      query(i).awaitTermination()
    }


    spark.stop()

    //    query.awaitTermination()

  }
}