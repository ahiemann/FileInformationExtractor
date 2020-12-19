import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, javadsl}
import akka.stream.scaladsl.{Flow, GraphDSL, RunnableGraph, Sink, Source}
import kafka.DocInformationsDeserializer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
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
  //var consumer = new Consumer()

  /*
  while(true) {
    println("Polling...")
    val records = consumer.consumer.poll(Duration.ofMillis(100))
    for (record <- records.asScala) {
      println(record)
    }
  }
  */

   // Spark

  val spark = SparkSession.builder.appName("Spark Streaming").config("spark.master", "local").getOrCreate()

  val scalaKafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[DocInformationsDeserializer],
    "group.id" -> "something",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )
  val kafkaParams=scalaKafkaParams.asJava
  val sc = new StreamingContext(spark.sparkContext, Seconds(1))

  val topics = asJavaCollection(Array("extraction"))
  val offset = Map[TopicPartition, java.lang.Long]((new TopicPartition("data",1),1L)).asJava

  val kafkaStream = KafkaUtils.createDirectStream[String, String](sc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

  val docDataStream = kafkaStream.map(record => record.value)
  val docDataStream1Second = docDataStream.window(Seconds(1))

  docDataStream1Second.foreachRDD(rdd =>
    if (rdd.isEmpty()) {
      println("Keine neuen Daten")
    }
    else {
      println("neue Daten")
    }
  )
  // stream.foreachRDD(rdd => println( rdd.name))

  sc.start()
  sc.awaitTermination()
  spark.stop()



}
