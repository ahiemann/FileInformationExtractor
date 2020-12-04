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

  val fs = FileSystems.getDefault

  implicit val system = ActorSystem("Sys")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val fileSource:Source[Path, NotUsed] =
    Directory
    .ls(fs.getPath(directory))

  val filterFilesFlow:Flow[Path, Path, NotUsed] = Flow[Path].filter(p => Files.isRegularFile(p))

  val extractFulltextFlow:Flow[Path, String, NotUsed] = Flow.fromFunction((path:Path) => {
    val tika = new Tika()
    tika.setMaxStringLength(Int.MaxValue)
    tika.parseToString(path)
  })


  val result = fileSource.via(filterFilesFlow).via(extractFulltextFlow).runWith(Sink.seq[String])
  val fresult = Await.result(result, 30 seconds)
  println(fresult.size)
}
