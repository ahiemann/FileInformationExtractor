import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, javadsl}
import akka.stream.scaladsl.{Flow, GraphDSL, RunnableGraph, Sink, Source}
import streams.ProcessingStream

import scala.language.postfixOps





object Main extends App {

  val directoryIn = "./testdata"
  val directoryOut = "./src/main/scala/streams/"

  implicit val system: ActorSystem = ActorSystem("Sys")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val graph = ProcessingStream.getGraph(directoryIn, directoryOut)
  RunnableGraph.fromGraph(graph).run()

}
