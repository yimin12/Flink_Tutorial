package sink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/3 13:38
*   @Description : 
*
*/
object RedisSink {
  //需求：把netcat作为数据源，并且统计每个单词的次数，统计的结果写入Redis数据库中。
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    import org.apache.flink.streaming.api.scala._
    //读取数据源
    val stream: DataStream[String] = streamEnv.socketTextStream("hadoop101",8888)
    val result: DataStream[(String, Int)] = stream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)
    //把结果写入Redis中
    //设置连接Redis的配置
    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setDatabase(3).setHost("hadoop101").setPort(6379).build()

    result.addSink(new RedisSink[(String,Int)](config,new RedisMapper[(String, Int)] {
      // set the command for redis
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "t_wc")
      }

      // get the key from data stream
      override def getKeyFromData(t: (String, Int)): String = {

        t._1
      }

      // get the value
      override def getValueFromData(t: (String, Int)): String = {
        t._2 + ""
      }
    }))

    streamEnv.execute("redisSink")
  }
}
