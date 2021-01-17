package streams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, Graph}
import akka.stream.scaladsl.RunnableGraph
import org.scalatest.matchers.must.Matchers.convertToAnyMustWrapper
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import java.nio.file.{Files, Paths}
import java.time.Duration

import kafka.{DocInformationDeserializer, DocInformationSerializer}
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.tika.metadata.Metadata


class ProcessingStreamSpec extends AnyWordSpec with Matchers with EmbeddedKafka {

    implicit val system: ActorSystem = ActorSystem("Sys")
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val directoryIn = "./src/test/scala/streams/testfiles"
    val directoryOut = "./src/test/scala/streams/"
    val processingStream = new ProcessingStream(directoryIn, directoryOut)

    "The ProcessingStream" should {
      "extract text and metadata from files which placed in given directory" in {

        val graph = processingStream.getGraph
        RunnableGraph.fromGraph(graph).run()

        Thread.sleep(5000)

        val filePath = Paths.get("./src/test/scala/streams/out.txt")
        val firstLine = Files.lines(filePath).findFirst().get()
        Files.delete(filePath)

        Thread.sleep(5000)

        firstLine should startWith ("Hello World.")
      }

      "return a graph with Closed Shape and NotUsed" in {
        val graph = processingStream.getGraph
        RunnableGraph.fromGraph(graph).run()

        graph mustBe a [Graph[_, _]]
      }

      "work with kafka" in {
        withRunningKafka {
          implicit val serializer: DocInformationSerializer = new DocInformationSerializer
          implicit val deserializer: DocInformationDeserializer = new DocInformationDeserializer

          val TOPIC = "extraction"
          createCustomTopic(TOPIC)
          publishToKafka[(String, Metadata)](TOPIC, ("This is a test", new Metadata()))
          consumeFirstMessageFrom[(String, Metadata)](TOPIC)._1 shouldBe "This is a test"
        }
      }

      "Consumer records should be 0 if we give an empty tuple" in {
        val p = new Producer
        val pTuple = ("",new Metadata())
        val recordMeta = new ProducerRecord[String, (String,Metadata)](p.TOPIC, p.KEY, pTuple)
        p.producer.send(recordMeta)
        val c = new Consumer
        val records = c.consumer.poll(Duration.ofMillis(100))

        records.count() should be (0)
      }
    }
}