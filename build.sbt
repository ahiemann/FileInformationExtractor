name := "FileInformationExtractor"

version := "0.1"

scalaVersion := "2.12.10"

// Apache Tika related libraries
// https://mvnrepository.com/artifact/org.apache.tika/tika-parsers
libraryDependencies += "org.apache.tika" % "tika-parsers" % "1.24.1"


// local testing
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % "test"


// Akka
val AkkaVersion = "2.5.31"
libraryDependencies ++= {
  Seq(
    "com.typesafe.akka" %% "akka-actor" % AkkaVersion,
    "com.typesafe.akka" %% "akka-testkit" % AkkaVersion % Test
  )
}

// Alpakka
libraryDependencies ++= Seq(
  "com.lightbend.akka" %% "akka-stream-alpakka-file" % "2.0.2",
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
)

// Kafka
// libraryDependencies += "org.apache.kafka" %% "kafka" % "2.6.0"
// Embedded Kafka for testing
libraryDependencies += "io.github.embeddedkafka" %% "embedded-kafka" % "2.4.1"


// Spark
libraryDependencies += "org.apache.spark" % "spark-core_2.12" % "3.0.1"
//libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % "3.0.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % "3.0.1"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % "3.0.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "3.0.1"