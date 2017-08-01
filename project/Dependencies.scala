import sbt._

object Dependencies {
	lazy val sparkVersion 		 = "2.2.0"
	lazy val sparkStreaming 	 = "org.apache.spark" % "spark-streaming_2.11"            % sparkVersion
	lazy val sparkStreamingKafka = "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % sparkVersion
	lazy val sparkSql 			 = "org.apache.spark" % "spark-sql_2.11"                  % sparkVersion
}