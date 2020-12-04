package timeInterval
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import source.StationLog
/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/3 19:49
*   @Description : 
*
*/
object LatenessDataOnWindow {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    streamEnv.setParallelism(1)
    //设置时间语义
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream:DataStream[StationLog] = streamEnv.socketTextStream("hadoop101",8888).map(line=>{
      var arr = line.split(",")
      new StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[StationLog]() {
      override def extractTimestamp(t: StationLog): Long = t.callTime
    })
    // define a side output
    var lateTag = new OutputTag[StationLog]("late")
    // divide and open window
    var result: DataStream[String] = stream.keyBy(_.sid).timeWindow(Time.seconds(10), Time.seconds(5))
    //设置迟到的数据超出了2秒的情况下，怎么办。交给AllowedLateness处理
    //也分两种情况，第一种：允许数据迟到5秒（迟到2-5秒），再次延迟触发窗口函数。触发的条是：Watermark < end-of-window + allowedlateness
    //第二种：迟到的数据在5秒以上，输出到则流中
      .allowedLateness(Time.seconds(5)).sideOutputLateData(lateTag).aggregate(new MyAggregateCountFunction, new OutputResultWindowFunction)

    result.getSideOutput(lateTag).print("late")
    result.print("main")
    streamEnv.execute()
  }

  class MyAggregateCountFunction extends AggregateFunction[StationLog, Long, Long]{
    override def createAccumulator(): Long = 0

    override def add(in: StationLog, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  class OutputResultWindowFunction extends WindowFunction[Long, String, String , TimeWindow]{
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[String]): Unit = {
      var value = input.iterator.next()
      var sb = new StringBuilder
      sb.append("Window's range: ").append(window.getStart).append("-----").append(window.getEnd)
      sb.append("\n")
      sb.append("current base's id :").append(key).append(", the number of calling is ").append(value)
      out.collect(sb.toString())
    }
  }
}
