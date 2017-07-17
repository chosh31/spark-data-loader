//import Dependencies._

val sparkVersion = "2.2.0"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.11.0",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "KafkaTest",
    libraryDependencies += "org.apache.spark" % "spark-streaming_2.11"            % sparkVersion,
    libraryDependencies += "org.apache.spark" % "spark-sql_2.11"                  % sparkVersion,
    libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % sparkVersion
  )
