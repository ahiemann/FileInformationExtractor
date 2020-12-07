package streams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, Graph}
import akka.stream.scaladsl.{RunnableGraph, Source}
import org.scalatest.matchers.must.Matchers.convertToAnyMustWrapper
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers


class ProcessingStreamSpec extends AnyWordSpec with Matchers {

    implicit val system: ActorSystem = ActorSystem("Sys")
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val processingStream = new ProcessingStream
    val directoryIn = "./testdata"
    val directoryOut = "./src/test/scala/streams/"

    "The ProcessingStream" should {

      "extract text and metadata from files which placed in given directory" in {

        val graph = processingStream.getGraph(directoryIn, directoryOut)
        RunnableGraph.fromGraph(graph).run()

        val ipfileStream = getClass.getResourceAsStream("./out.txt")
        val readlines = scala.io.Source.fromInputStream(ipfileStream).getLines().mkString

        readlines should startWith ("Hallo")
      }

      "return a graph with Closed Shape and NotUsed" in {
        val graph = processingStream.getGraph(directoryIn, directoryOut)
        RunnableGraph.fromGraph(graph).run()

        graph mustBe a [Graph[ClosedShape.type, NotUsed]]
      }
    }
}
