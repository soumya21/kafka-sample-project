import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import java.util.{Collections, Properties}

object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    val brokers = "localhost:9092"
    val groupId = "test-group"
    val topic = "test-topic"

    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer = new KafkaConsumer[String, String](props)

    consumer.subscribe(Collections.singletonList(topic))

    while (true) {
      val records = consumer.poll(1000)
      records.forEach { record =>
        val jsonStr = record.value()

        println(s"Consumed record value= ${jsonStr}")

      }

    }


  }
}
