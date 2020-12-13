import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, javadsl}
import akka.stream.scaladsl.{Flow, GraphDSL, RunnableGraph, Sink, Source}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.tika.metadata.Metadata
import streams.ProcessingStream

import java.time.Duration
import java.util.Properties
import java.util
import scala.jdk.CollectionConverters.IterableHasAsScala
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
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "kafka.MyDeserializer")
  props.put("group.id", "something")

  val consumer = new KafkaConsumer[String, String](props)
  val TOPIC = "extraction"

  consumer.subscribe(util.Collections.singletonList(TOPIC))

  while(true) {
    println("Polling...")
    val records = consumer.poll(Duration.ofMillis(100))
    for (record <- records.asScala) {
      println(record)
    }
  }
}
