import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.core.fs.Path
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write
object MSKFlinkApp extends FlinkConsumerUtils {
  implicit val formats = Serialization.formats(org.json4s.NoTypeHints)

  def main(args: Array[String]): Unit = {
    // Load Configuration
    val config = ConfigFactory.load()
    val brokers = config.getString("kafka.brokers")
    val topic = config.getString("kafka.topic")
    val outputPath = config.getString("s3.path")
    val groupId = config.getString("kafka.groupId")
    val checkpoints = config.getString("kafka.checkpoint").toInt

    // SSL properties
    val sslTruststoreLocation = config.getString("ssl.truststore.location")
    val sslTruststorePassword = config.getString("ssl.truststore.password")
    val sslKeystoreLocation = config.getString("ssl.keystore.location")
    val sslKeystorePassword = config.getString("ssl.keystore.password")
    val sslKeyPassword = config.getString("ssl.key.password")

    // Set up the Flink environment: Initialize the Flink execution environment and enable checkpointing.
    val flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment
    flinkEnv.enableCheckpointing(checkpoints, CheckpointingMode.EXACTLY_ONCE)

    // Configure Kafka source: Set up the Kafka source with the necessary properties to connect to the Local/MSK cluster.
    val kafkaSource = KafkaSource.builder[(String, String)]()
      .setBootstrapServers(brokers)
      .setTopics(topic)
      .setGroupId(groupId)
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setDeserializer(new KeyValueDeserializer) // Custom Serialization to deserialize Kafka msg into ConsumerRecords[String,String]
      .setProperty("security.protocol", "SSL")
      .setProperty("ssl.truststore.location", sslTruststoreLocation) //contains the CA certificate that client trusts
      .setProperty("ssl.truststore.password", sslTruststorePassword)
      .setProperty("ssl.keystore.location", sslKeystoreLocation) // contain client certificate and private key
      .setProperty("ssl.keystore.password", sslKeystorePassword)
      .setProperty("ssl.key.password", sslKeyPassword) // Replace with key password
      .build()

    // Process the data: Define the data processing logic.
    val streamData = flinkEnv.fromSource(
      kafkaSource,
      WatermarkStrategy.noWatermarks(), "Kafka Source")

    val stream = streamData.map { records: (String, String) =>
      val (key, value) = records
      ProcessedData(key, value)
    }

    // Parse Json and filter records with weightgrams > 100
    val filteredStreamData: DataStream[Record] = stream
      .flatMap { processedData =>
        parseJson(processedData.value).toList
      }
      .filter(_.weightgrams > 100)
    filteredStreamData.print()

    // Sink the data: Optionally, write the processed data to a sink.
    val sink: SinkFunction[ProcessedData] = StreamingFileSink.forRowFormat(
        new Path(outputPath),
        (data: ProcessedData, stream: java.io.OutputStream) => {
          val json = write(data)
          stream.write(json.getBytes)
          stream.write('\n')
        }
      )
      .withRollingPolicy(DefaultRollingPolicy.builder()
        .withRolloverInterval(15 * 60 * 1000)
        .withInactivityInterval(5 * 60 * 1000)
        .withMaxPartSize(1024 * 1024 * 128)
        .build())
      .build()
    stream.addSink(sink)
    flinkEnv.execute("Flink Kafka Consumer")
  }
  def parseJson(jsonString: String): Option[Record] = {
    val objectMapper = new ObjectMapper()
    try {
      val node = objectMapper.readTree(jsonString)
      val id = node.get("ProductId").asText()
      val name = node.get("Name").asText()
      val weightgrams = node.get("WeightGrams").asInt()
      Some(Record(id, name, weightgrams))
    }
    catch {
      case _: Exception => None

    }

  }
}