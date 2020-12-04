package cep

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/2 23:01
*   @Description : 
*
*/
case class LoginEvent(id:Long, userName:String, eventType:String, eventTime:Long)
object TestCepByLogin {

  //从一堆的登录日志中，匹配一个恶意登录的模式（如果一个用户连续（在10秒内）失败三次，则是恶意登录），从而找到哪些用户名是恶意登录
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    import org.apache.flink.streaming.api.scala._
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // set the time phase

    val stream: DataStream[LoginEvent] = streamEnv.fromCollection(List(
      new LoginEvent(1, "张三", "fail", 1577080457),
      new LoginEvent(2, "张三", "fail", 1577080458),
      new LoginEvent(3, "张三", "fail", 1577080460),
      new LoginEvent(4, "李四", "fail", 1577080458),
      new LoginEvent(5, "李四", "success", 1577080462),
      new LoginEvent(6, "张三", "fail", 1577080462)
    )).assignAscendingTimestamps(_.eventTime*1000L) // //指定EventTime的时候必须要确保是时间戳（精确到毫秒）

    // define the pattern
    val pattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("start").where(_.eventType.equals("fail"))
      .next("fail2").where(_.eventType.equals("fail"))
      .next("fail3").where(_.eventType.equals("fail"))
      .within(Time.seconds(10)) //时间限制
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(stream.keyBy(_.userName),pattern) //根据用户名分组
    val result: DataStream[String] = patternStream.select(new PatternSelectFunction[LoginEvent, String] {
      override def select(map: util.Map[String, util.List[LoginEvent]]) = {
        val keyIter: util.Iterator[String] = map.keySet().iterator()
        val e1: LoginEvent = map.get(keyIter.next()).iterator().next()
        val e2: LoginEvent = map.get(keyIter.next()).iterator().next()
        val e3: LoginEvent = map.get(keyIter.next()).iterator().next()
        "用户名:" + e1.userName + "登录时间:" + e1.eventTime + ":" + e2.eventTime + ":" + e3.eventTime
      }
    })
    result.print()
    streamEnv.execute()
  }
}
