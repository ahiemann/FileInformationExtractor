package streams

import akka.NotUsed
import akka.stream.{ClosedShape, Graph}
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Sink, Zip}
import org.apache.tika.Tika
import org.apache.tika.metadata.Metadata

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
        val extractMetadataFlow = builder.add(
          Flow.fromFunction((path:Path) => {
            val tika = new Tika()
            var metaData = new Metadata()
            tika.parse(path, metaData)
            metaData
          })
        )

        val broadcast = builder.add(Broadcast[Path](2))
        val zipDocInfos = builder.add(Zip[String, Metadata])

        // sinks
        val finalDestination = Sink.seq[(String, Metadata)]
        // val finalDestination = Sink.foreach(println)


        // Graph setup
        fileSource ~> fileOnlyFilterFLow ~> broadcast
        broadcast.out(0) ~> extractFulltextFlow ~> zipDocInfos.in0
        broadcast.out(1) ~> extractMetadataFlow ~> zipDocInfos.in1


        zipDocInfos.out ~> finalDestination

        ClosedShape
    }
  }

}
