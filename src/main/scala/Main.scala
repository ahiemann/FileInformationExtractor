import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.RunnableGraph
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.tika.metadata.Metadata
import streams.ProcessingStream

import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.OffsetDateTime
import scala.language.postfixOps

object Main extends App {
  val directoryIn = "./testdata"
  val directoryOut = "./src/main/scala/streams/"

  implicit val system: ActorSystem = ActorSystem("Sys")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val processingStream = new ProcessingStream(directoryIn, directoryOut)
  val graph = processingStream.getGraph
  RunnableGraph.fromGraph(graph).run()

  // Spark
  val sparkConfig = new SparkConf().setMaster("local[*]").setAppName("FileInformationStream")
  val sc = new StreamingContext(sparkConfig, Seconds(1))
  sc.sparkContext.setLogLevel("ERROR")

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "kafka.DocInformationDeserializer",
    "group.id" -> "something",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  val topics = Array("extraction")
  val kafkaStream = KafkaUtils.createDirectStream[String, (String, Metadata)](
    sc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, (String, Metadata)](topics, kafkaParams)
  )

  val docDataStream = kafkaStream.map(record => record.value)
  val docDataStream1Second = docDataStream.window(Seconds(1))

  val dataStream = docDataStream1Second.map(record => {
    val metadata = record._2
    (record._1, metadata.get("resourceName"), metadata.get("Author"), metadata.get("date"), metadata.get("dc:format"))
  })

  dataStream.foreachRDD { rdd =>
    val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
    import spark.implicits._

    if (! rdd.isEmpty()) {
      println("New data received")

      val df = rdd.toDF()
      df.createOrReplaceTempView("DocInformationDataFrame")

      val countDataFrame = spark.sql("select count(*) from DocInformationDataFrame")
      countDataFrame.show()

      val completeDataFrame = spark.sql("select * from DocInformationDataFrame")
      completeDataFrame.show()

      val wordCounts = rdd.map(record => {
        val words = record._1.split("[ \n]")
        sc.sparkContext.parallelize(words.map(word => (word, 1))).reduceByKey(_ + _).sortBy(_._2)//.top(3)//.collect()
      })
      wordCounts.foreach(list => list.foreach(println))
      val sorted = wordCounts.first().sortBy(_._2)
      println
      println("Occurences of words in text")
      sorted.foreach(println)


      val docNameCount = rdd.map(record => { (record._2, 1) }).reduceByKey((a, b) => { a + b })
      println
      println("Stats about document names:")
      docNameCount.foreach(println)

      val docAuthorCount = rdd.map(record => { (record._3, 1) }).reduceByKey((a, b) => { a + b })
      println
      println("Stats about document authors:")
      docAuthorCount.foreach(println)

      val docYearCount = rdd.map(record => {
        if (record._4 != null) {
          // inspired by https://stackoverflow.com/a/46488425
          val dateTimeFormatter = new DateTimeFormatterBuilder()
            // date/time
            .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
            // offset (hh:mm - "+00:00" when it's zero)
            .optionalStart().appendOffset("+HH:MM", "+00:00").optionalEnd()
            // offset (hhmm - "+0000" when it's zero)
            .optionalStart().appendOffset("+HHMM", "+0000").optionalEnd()
            // offset (hh - "Z" when it's zero)
            .optionalStart().appendOffset("+HH", "Z").optionalEnd()
            .toFormatter()

          val date = OffsetDateTime.parse(record._4, dateTimeFormatter)
          (date.getYear, 1)
        }
        else {
          // placeholder for "no date could be extracted"...
          (0, 1)
        }
      }
      ).reduceByKey((a, b) => { a + b })
      println
      println("Stats about document creation years:")
      docYearCount.foreach(println)

      val docTypeCount = rdd.map(record => { (record._5, 1) }).reduceByKey((a, b) => { a + b })
      println
      println("Stats about document types:")
      docTypeCount.foreach(println)
    }
  }

  sc.start()
  sc.awaitTermination()
}
