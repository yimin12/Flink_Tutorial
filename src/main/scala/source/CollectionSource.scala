package source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/2 17:15
*   @Description : 
*
*/
case class StationLog(sid:String, var callOut:String, var callIn:String, callType:String, callTime:Long, duration:Long)
object CollectionSource {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    import org.apache.flink.streaming.api.scala._
    val stream: DataStream[StationLog] = streamEnv.fromCollection(Array(
      new StationLog("001", "1866", "189", "busy", System.currentTimeMillis(), 0),
      new StationLog("002", "1866", "188", "busy", System.currentTimeMillis(), 0),
      new StationLog("004", "1876", "183", "busy", System.currentTimeMillis(), 0),
      new StationLog("005", "1856", "186", "success", System.currentTimeMillis(), 20)
    ))
    stream.print()
    streamEnv.execute()
  }
}
