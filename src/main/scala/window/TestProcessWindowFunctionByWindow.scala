package window
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import source.StationLog

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/3 20:43
*   @Description : 
*
*/
object TestProcessWindowFunctionByWindow {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment =StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    streamEnv.setParallelism(1)
    val stream: DataStream[StationLog] = streamEnv.socketTextStream("hadoop101",8888).map(
      line =>{
        var arr = line.split(",")
        new StationLog(arr(0).trim,arr(1).trim,arr(2).trim,arr(3).trim,arr(4).trim.toLong,arr(5).trim.toLong)
      }
    )
    stream.map(log =>{
      (log.sid, 1)
    }).keyBy(_._1).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).process(new ProcessWindowFunction[(String, Int), (String, Long), String, TimeWindow] {
      override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Long)]): Unit = {
        print("-------------")
        out.collect((key, elements.size))
      }
    }).print()
    streamEnv.execute()
  }
}
