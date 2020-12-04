package transformation

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import source.StationLog

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/2 21:59
*   @Description : 
*
*/
object TestSideOutputStream {

  import org.apache.flink.streaming.api.scala._
  var notSuccessTag = new OutputTag[StationLog]("not_success")

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    var filePath = getClass.getResource("/station.log").getPath
    val stream: DataStream[StationLog] = streamEnv.readTextFile(filePath).map(line=>{
      var arr = line.split(",")
      new StationLog(arr(0).trim,arr(1).trim,arr(2).trim,arr(3).trim,arr(4).trim.toLong,arr(5).trim.toLong)
    })
    val result: DataStream[StationLog] = stream.process(new CreateSideOutputStream(notSuccessTag))
    result.print("MainStream")
    // get the side streaming based on main stream
    val sideStream: DataStream[StationLog] = result.getSideOutput(notSuccessTag)
    sideStream.print("Side streaming")
    streamEnv.execute()
  }

  class CreateSideOutputStream(tag: OutputTag[StationLog]) extends ProcessFunction[StationLog, StationLog]{
    override def processElement(value: StationLog, ctx: ProcessFunction[StationLog, StationLog]#Context, collector: Collector[StationLog]): Unit = {
      if(value.callType.equals("success")){
        // main stream
        collector.collect(value)
      } else {
        ctx.output(tag, value)
      }
    }
  }
}
