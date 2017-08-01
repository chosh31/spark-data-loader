package example

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

class DkyBetaScribe(sparkConf: SparkConf, kafkaParams: Map[String, Object]) extends BaseScribe(sparkConf, kafkaParams) {
	override def batchDuration: Duration = Seconds(2)
	override def topics: Set[String] = Set("log-dky-beta-problem")
	override def partitionFields: List[String] = List("kafka_type")
}