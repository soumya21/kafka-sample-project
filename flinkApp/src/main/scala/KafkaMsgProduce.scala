
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.utils.Utils.sleep

import java.util.Properties
object KafkaMsgProduce extends App {
  val newArgs = Array("20", "test-topic", "localhost:9092")
  val events = newArgs(0).toInt
  val topic = newArgs(1)
  val brokers = newArgs(2)

  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "producer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


  val producer = new KafkaProducer[String, String](props)
  val t = System.currentTimeMillis()
  for (nEvents <- Range(1, events + 1)) {

    val key = "messageKey " + nEvents.toString
    val msg = "test message " + nEvents.toString

    val data = new ProducerRecord[String, String](topic, key, msg)
    producer.send(data)
    println(s"key=$key: msg= $msg")
    sleep(2000)
  }

  println("sent per second: " + ((events * 1000).toFloat / (System.currentTimeMillis() - t)))
  producer.close()

}
