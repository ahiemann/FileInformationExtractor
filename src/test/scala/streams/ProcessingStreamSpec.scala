package streams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, Graph}
import akka.stream.scaladsl.RunnableGraph
import org.scalatest.matchers.must.Matchers.convertToAnyMustWrapper
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}


class ProcessingStreamSpec extends AnyWordSpec with Matchers {

    implicit val system: ActorSystem = ActorSystem("Sys")
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val directoryIn = "./testdata"
    val directoryOut = "./src/test/scala/streams/"

    "The ProcessingStream" should {

      "extract text and metadata from files which placed in given directory" in {

        val graph = ProcessingStream.getGraph(directoryIn, directoryOut)
        RunnableGraph.fromGraph(graph).run()

        Thread.sleep(5000)

        val firstLine = Files.lines(Paths.get("./src/test/scala/streams/out.txt")).findFirst().get()

        Thread.sleep(5000)

        firstLine should startWith ("Hallo")
      }

      "return a graph with Closed Shape and NotUsed" in {
        val graph = ProcessingStream.getGraph(directoryIn, directoryOut)
        RunnableGraph.fromGraph(graph).run()

        graph mustBe a [Graph[ClosedShape.type, NotUsed]]
      }
    }
}
