package transformation

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import source.StationLog

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/2 21:08
*   @Description : 
*
*/
object TestProcessFunction {
  //监控每一个手机号码，如果这个号码在5秒内，所有呼叫它的日志都是失败的，则发出告警信息
  //如果在5秒内只要有一个呼叫不是fail则不用告警
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val stream: DataStream[StationLog] = streamEnv.socketTextStream("hadoop101", 8888).map(line => {
      var arr = line.split(",")
      new StationLog(arr(0).trim,arr(1).trim,arr(2).trim,arr(3).trim,arr(4).trim.toLong,arr(5).trim.toLong)
    })
    val result: DataStream[String] = stream.keyBy(_.callIn).process(new MonitorCallFail)
    result.print()
    streamEnv.execute()
  }

  class MonitorCallFail extends KeyedProcessFunction[String, StationLog, String]{
    lazy val timeState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time", classOf[Long]))

    override def processElement(value: StationLog, ctx: KeyedProcessFunction[String, StationLog, String]#Context, collector: Collector[String]): Unit = {

      var time = timeState.value()
      if(time == 0 && value.callType.equals("fail")){
        var nowTime = ctx.timerService().currentProcessingTime()
        var onTime = nowTime + 8*1000L
        ctx.timerService().registerProcessingTimeTimer(onTime)
        timeState.update(onTime)
      }
      if(time != 0 && !value.callType.equals("fail")){
        ctx.timerService().deleteProcessingTimeTimer(time)
        timeState.clear() // clear the state
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, StationLog, String]#OnTimerContext, out: Collector[String]): Unit = {
      var warnStr = "trigger time : " + timestamp + " cell phone : " + ctx.getCurrentKey
      out.collect(warnStr)
      timeState.clear()
    }
  }
}
