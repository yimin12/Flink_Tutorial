package window
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import source.StationLog

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/3 20:33
*   @Description : 
*
*/
object TestAggregatFunctionByWindow {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    //读取数据源
    val stream: DataStream[StationLog] = streamEnv.socketTextStream("hadoop101",8888)
      .map(line=>{
        var arr=line.split(",")
        new StationLog(arr(0).trim,arr(1).trim,arr(2).trim,arr(3).trim,arr(4).trim.toLong,arr(5).trim.toLong)
      })
    stream.map(log=>((log.sid,1))).keyBy(_._1).window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))) // sliding window with size 5
      .aggregate(new MyAggregateFunction, new MyWindowFunction).print()

    streamEnv.execute()
  }

  class MyAggregateFunction extends AggregateFunction[(String,Int),Long,Long]{
    override def createAccumulator(): Long = 0

    override def add(in: (String, Int), acc: Long): Long = acc + in._2

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  class MyWindowFunction extends WindowFunction[Long, (String, Long), String, TimeWindow]{
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[(String, Long)]): Unit = {
      out.collect((key,input.iterator.next())) //next得到第一个值，迭代器中只有一个值
    }
  }
}
