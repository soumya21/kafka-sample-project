import com.typesafe.config.ConfigFactory
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode

object SampleFlinkApp {
  def main(args: Array[String]): Unit = {
    // Load Configuration
//    val config = ConfigFactory.load()
//    val brokers = config.getString("kafka.brokers")
//    val topic = config.getString("kafka.topic")
//    val groupId = config.getString("kafka.groupId")
//    val checkpoints = config.getString("kafka.checkpoint").toInt

    val brokers = "localhost:9092"
    val groupId = "test-group"
    val topic = "test-topic"
    val checkpoints = 1000
    // Set up the execution environment
    val flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment
    flinkEnv.enableCheckpointing(checkpoints, CheckpointingMode.EXACTLY_ONCE)

    // Configure Kafka consumer properties
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", brokers)
    properties.setProperty("group.id", groupId)
    properties.setProperty("auto.offset.reset", "earliest")

    val kafkaSource = new FlinkKafkaConsumer[String](
      topic,
      new SimpleStringSchema(),
      properties
    )
    val stream = flinkEnv.addSource(kafkaSource)
    stream.print()


    flinkEnv.execute("Flink Kafka Consumer")


  }
}
