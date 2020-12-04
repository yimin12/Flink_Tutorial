package timeInterval
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import source.StationLog
/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/3 20:15
*   @Description : 
*
*/

/**
 * 每隔5秒统计一下最近10秒内，每个基站中通话时间最长的一次通话发生的时间还有，
 * 主叫号码，被叫号码，通话时长，并且还得告诉我们当前发生的时间范围（10秒）
 */
object MaxLongCallTime {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    streamEnv.setParallelism(1)
    //设置时间语义
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream: DataStream[StationLog] = streamEnv.socketTextStream("hadoop101",8888).map(line => {
      var arr = line.split(",")
      new StationLog(arr(0).trim,arr(1).trim,arr(2).trim,arr(3).trim,arr(4).trim.toLong,arr(5).trim.toLong)
    }).assignAscendingTimestamps(_.callTime) // 参数中指定Eventtime具体的值

    stream.filter(_.callType.equals("success")).keyBy(_.sid).timeWindow(Time.seconds(10), Time.seconds(5))
      .reduce(new MyReduceFunction(), new ReturnMaxTimeWindowFunction).print()
    streamEnv.execute()
  }

  class MyReduceFunction() extends ReduceFunction[StationLog]{
    override def reduce(t: StationLog, t1: StationLog): StationLog = {
      if(t.duration > t1.duration) t else t1
    }
  }

  class ReturnMaxTimeWindowFunction extends WindowFunction[StationLog, String, String, TimeWindow]{
    //在窗口触发的才调用一次
    override def apply(key: String, window: TimeWindow, input: Iterable[StationLog], out: Collector[String]): Unit = {
      var value = input.iterator.next()
      var sb = new StringBuilder
      sb.append("窗口范围是:").append(window.getStart).append("----").append(window.getEnd)
      sb.append("\n")
      sb.append("呼叫时间：").append(value.callTime)
        .append("主叫号码：").append(value.callOut)
        .append("被叫号码：").append(value.callIn)
        .append("通话时长：").append(value.duration)
      out.collect(sb.toString())
    }
  }
}
