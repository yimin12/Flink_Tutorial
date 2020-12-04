package sink

import java.lang
import java.util.Properties

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/3 13:21
*   @Description : 
*
*/
object KafkaSinkByKeyValue {

  //Kafka作为Sink的第二种（KV）
  //把netcat作为数据源，统计每个单词的数量，并且把统计的结果写入Kafka
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._;
    streamEnv.setParallelism(1)

    // read the data source
    val stream: DataStream[String] = streamEnv.socketTextStream("hadoop101",8888)
    // calculate
    val result: DataStream[(String,Int)] = stream.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)

    // connect kafka
    var props = new Properties()
    props.setProperty("bootstrap.servers","hadoop101:9092, hadoop102:9092, hadoop103.9092")
    // create kafka sink
    var kafkaSink=new FlinkKafkaProducer[(String,Int)](
      "yimin_2020", new KafkaSerializationSchema[(String, Int)] {
        override def serialize(element: (String, Int),time: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
          new ProducerRecord("test_2020", element._1.getBytes,(element._2 + "").getBytes)
        }
      },
      props,
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE // exact_once:-> at_least_once -> at_most_once
    )
    result.addSink(kafkaSink)
    streamEnv.execute("Kafka's sink")
  }
}
