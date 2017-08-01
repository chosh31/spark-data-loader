package example

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf

object Main {
	def generateKafkaParams(brokers: String): Map[String, Object] = {
	// Create direct kafka stream with brokers and topics]
		val kafkaParams = Map(
			"bootstrap.servers" -> brokers,
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer],
			"group.id" -> "test-consumer",
			"auto.offset.reset" -> "latest",
			"enable.auto.commit" -> (false: java.lang.Boolean)
		)
		kafkaParams
	}

	def main(args: Array[String]) {
		// if (args.length < 1) {
		// 	System.err.println(s"""
		// 		|Usage: Main <brokers>
		// 		|  <brokers> is a list of one or more Kafka brokers
		// 		|
		// 		""".stripMargin)
		// 	System.exit(1)
		// }

		val Array(brokers) = Array("localhost:9092")
		// val Array(brokers) = args

		// args.foreach(println(_))

		val sparkConf = new SparkConf().setAppName("Main").set("spark.hadoop.outputCommitCoordination.enabled", "false").set("spark.driver.allowMultipleContexts","true").setMaster("local[4]")

		val kafkaParams = generateKafkaParams(brokers)

		val scribe = new DkyBetaScribe(sparkConf, kafkaParams)

		scribe.process("/home/ec2-user/parquet/partitionRefactorTest")

		scribe.streamingContext.start()
		scribe.streamingContext.awaitTermination()
	}
}