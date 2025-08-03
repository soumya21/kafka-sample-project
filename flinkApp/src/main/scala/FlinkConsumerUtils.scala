import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord


case class FlinkConsumerUtils()

case class ProcessedData(key: String, value: String)

case class Record(id: String, name: String, weightgrams: Int)

class KeyValueDeserializer extends KafkaRecordDeserializationSchema[(String, String)] {
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]], out: Collector[(String, String)]): Unit = {
    val key = Option(record.key()).map(new String(_, "UTF-8")).getOrElse("")
    val value = Option(record.value()).map(new String(_, "UTF-8")).getOrElse("")
    out.collect((key, value))

  }

  override def getProducedType: org.apache.flink.api.common.typeinfo.TypeInformation[(String, String)] = {
    org.apache.flink.api.scala.createTypeInformation[(String, String)]
  }

}




