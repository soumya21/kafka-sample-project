import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.utils.Utils.sleep

import java.nio.file.{Files, Paths}
import java.util.Properties


object KafkaProducer extends App{
  val topic = "test-topic"
  val brokers = "localhost:9092"
  val jsonFilePath = "C:/CBA/Card Modenisation/Development/Data/product.json"

  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("key.serializer", classOf[StringSerializer].getName)
  props.put("value.serializer", classOf[StringSerializer].getName)

  val producer = new KafkaProducer[String, String](props)

  try {
    val jsonString = new String(Files.readAllBytes(Paths.get(jsonFilePath)))
    val record = new ProducerRecord[String, String](topic, jsonString)
    producer.send(record)
    println(s"Sent record to topic $topic: $jsonString")
    sleep(2000)
  } catch {
    case e: Exception => println("Error sending record to Kafka", e)
  } finally {
    producer.close()
  }

}
