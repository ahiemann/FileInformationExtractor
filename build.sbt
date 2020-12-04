name := "FileInformationExtractor"

version := "0.1"

scalaVersion := "2.13.4"

// Apache Tika related libraries
// https://mvnrepository.com/artifact/org.apache.tika/tika-parsers
libraryDependencies += "org.apache.tika" % "tika-parsers" % "1.25"
// https://mvnrepository.com/artifact/org.apache.pdfbox/jbig2-imageio
libraryDependencies += "org.apache.pdfbox" % "jbig2-imageio" % "3.0.0"


// local testing
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % "test"

// Akka
libraryDependencies ++= {
  Seq(
    "com.typesafe.akka" %% "akka-actor" % "2.5.23",
    "com.typesafe.akka" %% "akka-testkit" % "2.5.23" % Test
  )
}

// Alpakka
val AkkaVersion = "2.5.23"
libraryDependencies ++= Seq(
  "com.lightbend.akka" %% "akka-stream-alpakka-file" % "2.0.2",
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion
)

