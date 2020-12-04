package source

import java.util.Properties
import java.util.Properties

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
//2、导入隐式转换
import org.apache.flink.streaming.api.scala._

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/2 19:48
*   @Description : 
*
*/
object KafkaSourceByKeyValue {
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //修改并行度
    streamEnv.setParallelism(1) //默认所有算子的并行度为1


    //连接Kafka的属性
    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop101:9092,hadoop102:9092,hadoop103:9092")
    props.setProperty("group.id","flink002")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("auto.offset.reset","latest")

    //设置Kafka数据源
    val stream: DataStream[(String, String)] = streamEnv.addSource(new FlinkKafkaConsumer[(String,String)]("yimin_kafka_test",new MyKafkaReader,props))
    stream.print()
    streamEnv.execute()
  }

  class MyKafkaReader extends KafkaDeserializationSchema[(String, String)]{
    // is the end of the stream
    override def isEndOfStream(t: (String, String)): Boolean = {
      false
    }

    // deserialization
    override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String) = {
      if(record != null){
        var key = "null"
        var value = "null"
        if(record.key() != null){
          key = new String(record.key(), "UTF-8")
        }
        if(record.value() != null){
          value = new String(record.value(), "UTF-8")
        }
        (key, value)
      } else {
        ("null", "null")
      }
    }

    override def getProducedType: TypeInformation[(String, String)] = {
      createTuple2TypeInformation(createTypeInformation[String],createTypeInformation[String])
    }
  }
}
