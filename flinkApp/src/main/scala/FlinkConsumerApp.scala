
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

object FlinkConsumerApp extends FlinkConsumerUtils {
  implicit val formats = Serialization.formats(org.json4s.NoTypeHints)

  def main(args: Array[String]): Unit = {
    println("hi")
    // Load Configuration
    val config = ConfigFactory.load()
    val brokers = config.getString("kafka.brokers")
    val topic = config.getString("kafka.topic")
    val outputPath = config.getString("output.path")
    val groupId = config.getString("kafka.groupId")
    val checkpoints = config.getString("kafka.checkpoint").toInt

    //1.Set up the Flink environment: Initialize the Flink execution environment and enable checkpointing.
    val flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment
    flinkEnv.enableCheckpointing(checkpoints, CheckpointingMode.EXACTLY_ONCE)

    // 2.Configure Kafka source: Set up the Kafka source with the necessary properties to connect to the Local/MSK cluster.
    val kafkaSource = KafkaSource.builder[(String, String)]()
      .setBootstrapServers(brokers)
      .setTopics(topic)
      .setGroupId(groupId)
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setDeserializer(new KeyValueDeserializer) //Custom Serialization to deserialize Kafka msg into ConsumerRecords[String,String]
      .build()

    //3.Process the data: Define the data processing logic.
    val stream = flinkEnv.fromSource(
      kafkaSource,
      WatermarkStrategy.noWatermarks(), "Kafka Source")
      .map { records: (String, String) =>
        val (key, value) = records
        ProcessedData("key= " + key, "value= " + value)
      }
        //Just for Debugging the values from topic
        println("Consumed Messages are: ")
    stream.print()

    //Parse Json and filter records with weightgrams >100 Todo PENDING TO TEST Computation
    val filteredStreamData: DataStream[Record] = stream
      .flatMap { processedData =>
        parseJson(processedData.value).toList
      }
      .filter(_.weightgrams > 100)
    filteredStreamData.print()
    //=================================================================================


    //4.Sink the data: Optionally, write the processed data to a sink.
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
