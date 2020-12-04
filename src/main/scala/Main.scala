import java.nio.file.{FileSystem, FileSystems, Files, Path}
import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.actor.TypedActor.dispatcher
import akka.stream.ActorMaterializer
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.Sink.seq
import akka.stream.scaladsl.{FileIO, Flow, Sink, Source}
import akka.util.ByteString
import org.apache.tika.Tika
import scala.language.postfixOps

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}



object Main extends App {

  val directory = "./testdata"
  var sinkData = Vector[String]()

  val fs = FileSystems.getDefault

  implicit val system = ActorSystem("Sys")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val fileSource:Source[Path, NotUsed] =
    Directory
    .ls(fs.getPath(directory))

  val filterFilesFlow:Flow[Path, Path, NotUsed] = Flow[Path].filter(p => Files.isRegularFile(p))

  val readFileFlow:Flow[Path, ByteString, NotUsed] = Flow[Path].flatMapConcat(path => {
    FileIO.fromPath(path).reduce((a, b) => a ++ b)
  })


  val extractFulltextFlow:Flow[Path, String, NotUsed] = Flow.fromFunction((path:Path) => {
    val tika = new Tika()
    tika.setMaxStringLength(Int.MaxValue)
    tika.parseToString(path)
  })


  // val consoleSink2:Sink[String, Future[Done]] = Sink.collection

  val result = fileSource.via(filterFilesFlow).via(extractFulltextFlow).runWith(Sink.seq[String])
  val fresult = Await.result(result, 30 seconds)
  println(fresult.size)

/*
  val numbers = 1 to 1000
  //We create a Source that will iterate over the number sequence
  val numberSource: Source[Int, NotUsed] = Source.fromIterator(() => numbers.iterator)
  //Only let pass even numbers through the Flow
  val isEvenFlow: Flow[Int, Int, NotUsed] = Flow[Int].filter((num) => num % 2 == 0)
  //Create a Source of even random numbers by combining the random number Source with the even number filter Flow
  val evenNumbersSource: Source[Int, NotUsed] = numberSource.via(isEvenFlow)
  //A Sink that will write its input onto the console
  val consoleSink: Sink[Int, Future[Done]] = Sink.foreach[Int](println)
  //Connect the Source with the Sink and run it using the materializer
  evenNumbersSource.runWith(consoleSink)
*/
}
