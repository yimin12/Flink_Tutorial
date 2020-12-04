package source
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.util.Random

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/2 20:33
*   @Description : 
*
*/
object MyKafkaProducer {

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop101:9092,hadoop102:9092,hadoop103:9092")
    props.setProperty("key.serializer",classOf[StringSerializer].getName)
    props.setProperty("value.serializer",classOf[StringSerializer].getName)
    var producer = new KafkaProducer[String, String](props)
    var r = new Random()
    while(true){
      val data = new ProducerRecord[String, String]("yimin_test_kafka", "key" + r.nextInt(10), "value" + r.nextInt(100))
      producer.send(data)
      Thread.sleep(1000)
    }
    producer.close()
  }
}
