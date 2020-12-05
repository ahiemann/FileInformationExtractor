import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, javadsl}
import akka.stream.scaladsl.{Flow, GraphDSL, RunnableGraph, Sink, Source}
import streams.ProcessingStream

import scala.language.postfixOps





object Main extends App {

  val directory = "./testdata"

  implicit val system: ActorSystem = ActorSystem("Sys")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val graph = ProcessingStream.getGraph(directory)
  RunnableGraph.fromGraph(graph).run()
}
