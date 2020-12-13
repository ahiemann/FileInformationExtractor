package streams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, Graph}
import akka.stream.scaladsl.RunnableGraph
import kafka.{MyDeserializer, MySerializer}
import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.matchers.must.Matchers.convertToAnyMustWrapper
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.tika.metadata.Metadata


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

      "work with kafka" in {
        withRunningKafka {
          implicit val serializer: MySerializer = new MySerializer
          implicit val deserializer: MyDeserializer = new MyDeserializer

          val TOPIC = "extraction"
          createCustomTopic(TOPIC)
          publishToKafka[(String, Metadata)](TOPIC, ("This is a test", new Metadata()))
          consumeFirstMessageFrom[(String, Metadata)](TOPIC)._1 shouldBe "This is a test"
        }
      }
    }
}
