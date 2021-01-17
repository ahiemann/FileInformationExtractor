package streams

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}

import java.util.{Collections, Properties}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.tika.metadata.Metadata

class Producer {
  val TOPIC = "extraction"
  val KEY = "data"

  // Kafka definitions
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "kafka.DocInformationSerializer")


  val adminProps = new Properties()
  adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  val kAdmin: AdminClient = AdminClient.create(adminProps)
  val topic = new NewTopic(TOPIC, 1, 1.toShort)
  println("Before creation:")
  kAdmin.listTopics().names().get().forEach(println)
  if (! kAdmin.listTopics().names().get().contains(TOPIC)) {
    kAdmin.createTopics(Collections.singleton(topic))
  }
  else {
    println("Topic already existed")
  }
  println("After creation:")
  kAdmin.listTopics().names().get().forEach(println)
  kAdmin.close()


  val producer = new KafkaProducer[String, (String,Metadata)](props)


}