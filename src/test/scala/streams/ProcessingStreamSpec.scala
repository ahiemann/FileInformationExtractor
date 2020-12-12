package streams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, Graph}
import akka.stream.scaladsl.RunnableGraph
import org.scalatest.matchers.must.Matchers.convertToAnyMustWrapper
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import java.nio.file.{Files, Paths}
import java.util.Properties

import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


class ProcessingStreamSpec extends AnyWordSpec with Matchers with EmbeddedKafka {

    implicit val system: ActorSystem = ActorSystem("Sys")
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val directoryIn = "./testdata"
    val directoryOut = "./src/test/scala/streams/"
    val processingStream = new ProcessingStream(directoryIn, directoryOut)

    "The ProcessingStream" should {
      "extract text and metadata from files which placed in given directory" in {

        val graph = processingStream.getGraph(directoryIn, directoryOut)
        RunnableGraph.fromGraph(graph).run()

        Thread.sleep(5000)

        val firstLine = Files.lines(Paths.get("./src/test/scala/streams/out.txt")).findFirst().get()

        Thread.sleep(5000)

        firstLine should startWith ("Hallo")
      }

      "return a graph with Closed Shape and NotUsed" in {
        val graph = processingStream.getGraph(directoryIn, directoryOut)
        RunnableGraph.fromGraph(graph).run()

        graph mustBe a [Graph[ClosedShape.type, NotUsed]]
      }

      "test" in {
        withRunningKafka{

          // Kafka definitions
          val props = new Properties()
          props.put("bootstrap.servers", "localhost:9092")
          props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
          props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
          props.put("value.serializer", "kafka.UserSerializer")



          println("Sending...")


        }
      }
    }
}
