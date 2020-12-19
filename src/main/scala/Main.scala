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

import java.time.Duration
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
    // implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[Metadata]

    if (rdd.isEmpty()) {
      println("Keine neuen Daten")
    }
    else {
      println("neue Daten")
      val df = rdd.toDF()
      df.createOrReplaceTempView("DocInformationDataFrame")

      val countDataFrame = spark.sql("select count(*) from DocInformationDataFrame")
      countDataFrame.show()

      val completeDataFrame = spark.sql("select * from DocInformationDataFrame")
      completeDataFrame.show()
    }
    //spark.stop()
  }
  // stream.foreachRDD(rdd => println( rdd.name))

  sc.start()
  sc.awaitTermination()




}
