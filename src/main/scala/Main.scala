import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, javadsl}
import akka.stream.scaladsl.{Flow, GraphDSL, RunnableGraph, Sink, Source}
import streams.{Consumer, ProcessingStream}
import java.time.Duration



import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter
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
  var consumer = new Consumer()

  while(true) {
    println("Polling...")
    val records = consumer.consumer.poll(Duration.ofMillis(100))
    for (record <- records.asScala) {
      println(record)
    }
  }
}
