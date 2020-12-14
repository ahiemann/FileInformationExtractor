package streams

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.tika.metadata.Metadata

class Consumer {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "kafka.DocInformationsDeserializer")
  props.put("group.id", "something")

  val consumer = new KafkaConsumer[String, (String,Metadata)](props)
  val TOPIC = "extraction"

  consumer.subscribe(util.Collections.singletonList(TOPIC))
}