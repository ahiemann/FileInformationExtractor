package kafka

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.nio.charset.StandardCharsets
import java.util

import org.apache.kafka.common.serialization.Serializer
import org.apache.tika.metadata.Metadata

class DocInformationsSerializer extends Serializer[(String,Metadata)] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = { }

  override def serialize(topic: String, data: (String,Metadata)): Array[Byte] = {
    try {
      val byteOut = new ByteArrayOutputStream()
      val objOut = new ObjectOutputStream(byteOut)
      objOut.writeObject(data)
      objOut.close()
      byteOut.close()
      byteOut.toByteArray
    }
    catch {
      case ex:Exception => throw new Exception(ex.getMessage)
    }
  }

  override def close():Unit = { }

}