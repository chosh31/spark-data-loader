package example

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

object DkyBetaKafkaProblemTest {
  def main(args: Array[String]) {
    
    val sc = new SparkConf().setAppName("Main").set("spark.hadoop.outputCommitCoordination.enabled", "false").set("spark.driver.allowMultipleContexts","true").setMaster("local[4]")

    val sparkSession = SparkSession.builder().config(sc).getOrCreate()
    val sqlContext = sparkSession.sqlContext
    sqlContext.setConf("spark.sql.parquet.compression.codec.", "gzip")
    //val path = "/home/ec2-user/test"
    val path = "/home/ec2-user/parquet/problem/dky"

    val ssc = new StreamingContext(sc, Seconds(2))
    val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "localhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "kafka-problem-test",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
     )

    val topics = Array("log-dky-beta-problem")
    val inputStream = KafkaUtils.createDirectStream[String,String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
    )
    inputStream.foreachRDD { rdd =>
       val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition { iter =>
           val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
           //println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        }
       println("in problem looping")
       if (!rdd.isEmpty) {
           println("rdd exist!")
           val df = sqlContext.read/*.schema(schema)*/.json(rdd.map(x => x.value))
           println(df)
           df.write
            .mode(SaveMode.Append)
            //.partitionBy(partitionFields:_*)
             .format("parquet")
            .save(path)
        }
     }
    ssc.start()
    ssc.awaitTermination()

  }
}


