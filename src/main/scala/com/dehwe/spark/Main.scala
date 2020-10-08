package com.dehwe.spark

import org.apache.hadoop.hbase.client.{ConnectionFactory, Get}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

case class RawReview(customerId: Int, customerName: String, stars: Int)
case class EnrichedReview(customerId: Int, customerName: String, stars: Int, reputation: Int)

object Main {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val appName = "MyStreamingApp"

  def main(args: Array[String]): Unit = {
    try {

      val spark = SparkSession.builder().appName(appName).master("local[*]").getOrCreate()
      spark.conf.set("spark.hadoop.dfs.client.use.datanode.hostname", "true")
      spark.conf.set("spark.hadoop.fs.defaultFS", "hdfs://quickstart.cloudera:8020")
      import spark.implicits._

      val bootstrapServers = args(0)

      val rawDf = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", "reviews")
        .load()
        .selectExpr("CAST(value AS STRING)")

      rawDf.printSchema()

      val rawDs = rawDf.as[RawReview]

      val enrichedDs = rawDs.mapPartitions(partitionOfRawReviews=> {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "XX.XXX.XXX.XXX")
        val connection = ConnectionFactory.createConnection(conf)
        val table = connection.getTable(TableName.valueOf("tim:users"))

        val partitionOfEnrichedReviews = partitionOfRawReviews.map(rawReview => {
          val get = new Get(Bytes.toBytes(rawReview.customerId.toString)).addFamily(Bytes.toBytes("f1"))
          val result = table.get(get)
          val reputation = Bytes.toInt(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("reputation")))
          EnrichedReview(rawReview.customerId, rawReview.customerName, rawReview.stars, reputation)
        })
        partitionOfEnrichedReviews
      })

      val query = enrichedDs.writeStream
        .outputMode(OutputMode.Append())
        .format("parquet")
        .option("path", "hdfs://quickstart.cloudera:8020/user/tim/reviews")
        .option("checkpointLocation", "hdfs://quickstart.cloudera:8020/user/tim/reviews_checkpoint")
        .trigger(Trigger.ProcessingTime("30 seconds"))
        .start()
    }
  }
}
