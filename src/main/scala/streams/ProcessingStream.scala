package streams

import java.io.{BufferedWriter, FileWriter}

import akka.NotUsed
import akka.stream.{ClosedShape, Graph}
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Sink, Zip}
import org.apache.tika.Tika
import org.apache.tika.metadata.Metadata
import java.nio.file.{FileSystems, Files, Path}




class ProcessingStream {
  def getGraph(directoryPathIn: String, directoryPathOut: String): Graph[ClosedShape.type, NotUsed] = {
    val fs = FileSystems.getDefault

    GraphDSL.create() {
      implicit builder: GraphDSL.Builder[NotUsed] =>
        import GraphDSL.Implicits._

        // output file
        val writer = new BufferedWriter(new FileWriter(directoryPathOut + "out.txt", true))

        // sources
        val fileSource = builder.add(Directory.ls(fs.getPath(directoryPathIn)))

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
            val metaData = new Metadata()
            tika.parse(path, metaData)
            metaData
          })
        )

        val broadcast = builder.add(Broadcast[Path](2))
        val zipDocInfos = builder.add(Zip[String, Metadata])

        // sinks
        val finalDestination = Sink.seq[(String, Metadata)]
        val writeSink = Sink.foreach[(String,Metadata)]{d => writer.write(s"${d._1} ${d._2}"); writer.flush()}

        // Graph setup
        fileSource ~> fileOnlyFilterFLow ~> broadcast

        broadcast.out(0) ~> extractFulltextFlow ~> zipDocInfos.in0
        broadcast.out(1) ~> extractMetadataFlow ~> zipDocInfos.in1

        zipDocInfos.out ~> writeSink

        ClosedShape
    }
  }

}
