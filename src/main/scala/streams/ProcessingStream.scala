package streams

import akka.NotUsed
import akka.stream.{ClosedShape, Graph}
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.{Flow, GraphDSL, Sink}
import org.apache.tika.Tika

import java.nio.file.{FileSystems, Files, Path}

object ProcessingStream {
  def getGraph(directoryPath: String): Graph[ClosedShape.type, NotUsed] = {
    val fs = FileSystems.getDefault

    GraphDSL.create() {
      implicit builder: GraphDSL.Builder[NotUsed] =>
        import GraphDSL.Implicits._

        // sources
        val fileSource = builder.add(Directory.ls(fs.getPath(directoryPath)))

        // flows
        val fileOnlyFilterFLow = builder.add(Flow[Path].filter(p => Files.isRegularFile(p)))
        val extractFulltextFlow = builder.add(
          Flow.fromFunction((path:Path) => {
            val tika = new Tika()
            tika.setMaxStringLength(Int.MaxValue)
            tika.parseToString(path)
          })
        )

        // sinks
        val finalDestination = Sink.seq[String]
        //val finalDestination = Sink.foreach(println)


        fileSource ~> fileOnlyFilterFLow ~> extractFulltextFlow ~> finalDestination

        ClosedShape
    }
  }

}
