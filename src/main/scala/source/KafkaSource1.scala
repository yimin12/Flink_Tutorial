package source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/2 19:35
*   @Description : 
*
*/
object KafkaSource1 {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    import org.apache.flink.streaming.api.scala._

    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop101:9092,hadoop102:9092,hadoop103:9092")
    props.setProperty("group.id","flink01")
    props.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer", classOf[StringDeserializer].getName)
    props.setProperty("auto.offset.rest","latest")
    val stream: DataStream[String] = streamEnv.addSource(new FlinkKafkaConsumer[String]("yimin_kafka_test", new SimpleStringSchema(), props))
    stream.print()
    streamEnv.execute()
  }
}
