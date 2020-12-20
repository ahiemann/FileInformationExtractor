import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, javadsl}
import akka.stream.scaladsl.{Flow, GraphDSL, RunnableGraph, Sink, Source}
import kafka.DocInformationsDeserializer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.tika.metadata.Metadata
import streams.{Consumer, ProcessingStream}

import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.{Duration, LocalDateTime, OffsetDateTime}
import scala.collection.JavaConverters.{asJavaCollection, iterableAsScalaIterableConverter, mapAsJavaMapConverter}
import scala.language.postfixOps


object Main extends App {

  val directoryIn = "./testdata"
  val directoryOut = "./src/main/scala/streams/"

  implicit val system: ActorSystem = ActorSystem("Sys")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val processingStream = new ProcessingStream(directoryIn, directoryOut)
  val graph = processingStream.getGraph(directoryIn, directoryOut)
  RunnableGraph.fromGraph(graph).run()

  // Kafka
  /*
  var consumer = new Consumer()


  while(true) {
    println("Polling...")
    val records = consumer.consumer.poll(Duration.ofMillis(100))
    for (record <- records.asScala) {
      println(record)
    }
  }*/

   // Spark
   val sparkConfig = new SparkConf().setMaster("local[*]").setAppName("FileInformationStream")
   val sc = new StreamingContext(sparkConfig, Seconds(1))
    sc.sparkContext.setLogLevel("ERROR")



  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[DocInformationsDeserializer],
    "group.id" -> "something",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
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

    if (rdd.isEmpty()) {
      println("Keine neuen Daten")
    }
    else {

      println("neue Daten")
      /*
      val df = rdd.toDF()
      df.createOrReplaceTempView("DocInformationDataFrame")

      val countDataFrame = spark.sql("select count(*) from DocInformationDataFrame")
      countDataFrame.show()

      val completeDataFrame = spark.sql("select * from DocInformationDataFrame")
      completeDataFrame.show()

      val textsDF = spark.sql("select _1 as text from DocInformationDataFrame")
      */
      // TODO: Count of the most frequent words in extracted texts

      val docNameCount = rdd.map(record => { (record._2, 1) }).reduceByKey((a, b) => { a + b })
      println("Stats about document names:")
      docNameCount.foreach(println)

      val docAuthorCount = rdd.map(record => { (record._3, 1) }).reduceByKey((a, b) => { a + b })
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
            .toFormatter();

          val date = OffsetDateTime.parse(record._4, dateTimeFormatter)
          (date.getYear, 1)
        }
        else {
          // placeholder for "no date could be extracted"...
          (0, 1)
        }
      }
      ).reduceByKey((a, b) => { a + b })
      println("Stats about document creation years:")
      docYearCount.foreach(println)

      val docTypeCount = rdd.map(record => { (record._5, 1) }).reduceByKey((a, b) => { a + b })
      println("Stats about document types:")
      docTypeCount.foreach(println)

      /*
      val texts = textsDF.rdd.collect()
      //val texts = rdd.collect().map {case (text, resourceName, author, date, format) => text}
      val resourceNames = rdd.collect().map {case (text, resourceName, author, date, format) => resourceName}
      val authors = rdd.collect().map {case (text, resourceName, author, date, format) => author}
      val dates = rdd.collect().map {case (text, resourceName, author, date, format) => date}
      val formats = rdd.collect().map {case (text, resourceName, author, date, format) => format}

      val dataAnalytics = new DataAnalytics
*/
      /*
      val countedAuthors = dataAnalytics.countDifferentAuthors(sc.sparkContext, authors)
      println("Different authors: " + countedAuthors)

      val countedWords = dataAnalytics.countWords(sc.sparkContext, texts)
      println("Different words: " + countedWords)

      val countedDates = dataAnalytics.countDates(sc.sparkContext, texts)
      println("Different dates: " + countedDates)

      val countedFormates = dataAnalytics.countFormats(sc.sparkContext, texts)
      println("Different formates: " + countedFormates)

      val countedResNames = dataAnalytics.countResNames(sc.sparkContext, texts)
      println("Different resource names: " + countedResNames)
*/
    }
    //spark.stop()
  }

  sc.start()
  sc.awaitTermination()
}
