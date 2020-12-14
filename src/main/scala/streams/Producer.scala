package streams

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.tika.metadata.Metadata

class Producer {

  // Kafka definitions
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "kafka.DocInformationsSerializer")

  val producer = new KafkaProducer[String, (String,Metadata)](props)
  val TOPIC = "extraction"
  val KEY = "data"

}