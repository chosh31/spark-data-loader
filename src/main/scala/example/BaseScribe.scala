package example

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

abstract class BaseScribe(sparkConf: SparkConf, kafkaParams: Map[String, Object]) {
	def batchDuration: Duration
	def topics: Set[String]
	def partitionFields: List[String]
	// def schema: StructType

	val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
	val sqlContext = sparkSession.sqlContext
	sqlContext.setConf("spark.sql.parquet.compression.codec.", "gzip")

	val streamingContext = new StreamingContext(sparkSession.sparkContext, batchDuration)

	val dsStream = KafkaUtils.createDirectStream[String, String](
		streamingContext,
		PreferConsistent,
		Subscribe[String, String](topics, kafkaParams)
	)

	def process(path: String) {
		dsStream.foreachRDD { rdd =>
			val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
			rdd.foreachPartition { iter =>
				val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
				//println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
			}

			if (!rdd.isEmpty) {
				println("rdd exist!")
				val df = sqlContext.read/*.schema(schema)*/.json(rdd.map(x => x.value))
				println(df)
				df.write
				.mode(SaveMode.Append)
				.partitionBy(partitionFields:_*)
				.format("parquet")
				.save(path)
			}
		}
	}
}