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
  // TODO: Here and in ProcessingStream.scala: Allow value of type (String, Metadata) and not only String. Therefore a "value.deserializer" resp. "value.serializer" for this type has to be provided?!
  val propsText = new Properties()
  propsText.put("bootstrap.servers", "localhost:9092")
  propsText.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  propsText.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  propsText.put("group.id", "something") // TODO: Something better than "something"?

  val propsMeta = new Properties()
  propsMeta.put("bootstrap.servers", "localhost:9092")
  propsMeta.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  propsMeta.put("value.deserializer", "kafka.MyDeserializer")
  propsMeta.put("group.id", "something") // TODO: Something better than "something"?

  val consumerText = new KafkaConsumer[String, String](propsText)
  val consumerMeta = new KafkaConsumer[String, String](propsMeta)
  val TOPIC = "extraction"

  consumerText.subscribe(util.Collections.singletonList(TOPIC))
  consumerMeta.subscribe(util.Collections.singletonList(TOPIC))

  while(true) {
    println("Polling...")
    val records = consumerText.poll(Duration.ofMillis(100))
    for (record <- records.asScala) {
      println(record)
    }

    val records1 = consumerMeta.poll(Duration.ofMillis(100))
    for (record1 <- records1.asScala) {
      println(record1)
    }
  }
}
