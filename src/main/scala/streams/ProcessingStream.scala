package streams

import akka.{Done, NotUsed}
import akka.stream.{ClosedShape, Graph}
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Sink, Source, Zip}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.tika.Tika
import org.apache.tika.metadata.Metadata

import java.nio.charset.Charset
import java.nio.file.{FileSystem, FileSystems, Files, Path, Paths, StandardOpenOption}
import java.util.Properties
import scala.concurrent.Future




class ProcessingStream(val directoryPathIn:String, val directoryPathOut:String) {
  private val fs: FileSystem = FileSystems.getDefault

  // Kafka definitions
  val producer = new Producer()

  // Akka Streams definitions
  // sources
  def fileSource:Source[Path, NotUsed] = Directory.ls(fs.getPath(directoryPathIn))

  // flows
  def fileOnlyFilterFlow:Flow[Path, Path, NotUsed] = Flow[Path].filter(p => Files.isRegularFile(p))

  def extractFulltextFlow:Flow[Path, String, NotUsed] = Flow.fromFunction((path:Path) => {
    val tika = new Tika()
    tika.setMaxStringLength(Int.MaxValue)
    tika.parseToString(path)
  })

  def extractMetadataFlow:Flow[Path, Metadata, NotUsed] = Flow.fromFunction((path:Path) => {
    val tika = new Tika()
    val metaData = new Metadata()
    tika.parse(path, metaData)
    metaData
  })

  // sinks
  def fileSink: Sink[(String, Metadata), Future[Done]] = Sink.foreach[(String,Metadata)] {
    val outputFilePath = Paths.get(s"$directoryPathOut/out.txt")
    d =>
      println("Write to file...")
      if (! Files.exists(outputFilePath)) Files.createFile(outputFilePath)
      Files.writeString(outputFilePath, s"${d._1} ${d._2}", Charset.forName("UTF-8"), StandardOpenOption.APPEND)
      val recordMeta = new ProducerRecord[String, (String,Metadata)](producer.TOPIC, producer.KEY, d)
      println("Sending...")
      producer.producer.send(recordMeta)
  }


  def getGraph(directoryPathIn: String, directoryPathOut: String): Graph[ClosedShape.type, NotUsed] = {

    GraphDSL.create() {
      implicit builder: GraphDSL.Builder[NotUsed] =>
        import GraphDSL.Implicits._

        // sources
        val files = builder.add(fileSource)

        // flows
        val fileOnlyFilter = builder.add(fileOnlyFilterFlow)
        val extractFulltext = builder.add(extractFulltextFlow)
        val extractMetadata = builder.add(extractMetadataFlow)

        val broadcast = builder.add(Broadcast[Path](2))
        val zipDocInfos = builder.add(Zip[String, Metadata])

        // sinks
        val finalDestination = Sink.seq[(String, Metadata)]
        val writeSink = builder.add(fileSink)

        // Graph setup
        files ~> fileOnlyFilter ~> broadcast
                                            broadcast.out(0) ~> extractFulltext ~> zipDocInfos.in0
                                            broadcast.out(1) ~> extractMetadata ~> zipDocInfos.in1
                                                                                       zipDocInfos.out ~> writeSink

        ClosedShape
    }
  }

}
