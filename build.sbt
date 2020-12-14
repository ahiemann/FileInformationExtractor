name := "FileInformationExtractor"

version := "0.1"

scalaVersion := "2.12.10"

// Apache Tika related libraries
// https://mvnrepository.com/artifact/org.apache.tika/tika-parsers
libraryDependencies += "org.apache.tika" % "tika-parsers" % "1.25"
// https://mvnrepository.com/artifact/org.apache.pdfbox/jbig2-imageio
libraryDependencies += "org.apache.pdfbox" % "jbig2-imageio" % "3.0.0"


// local testing
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % "test"


// Akka
val AkkaVersion = "2.5.23"
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
  // "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
)

// Kafka
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.6.0"
// Embedded Kafka for testing
libraryDependencies += "net.manub" %% "scalatest-embedded-kafka" % "2.0.0" % "test"

// Embedded Kafka for testing
libraryDependencies += "net.manub" %% "scalatest-embedded-kafka" % "2.0.0" % "test"
