package kafka

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util

import org.apache.kafka.common.serialization.Deserializer
import org.apache.tika.metadata.Metadata

class MyDeserializer extends Deserializer[Metadata] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): Metadata = {
    val byteIn = new ByteArrayInputStream(data)
    val objIn = new ObjectInputStream(byteIn)
    val obj = objIn.readObject().asInstanceOf[Metadata]
    byteIn.close()
    objIn.close()
    obj
  }

  override def close(): Unit = { }
}