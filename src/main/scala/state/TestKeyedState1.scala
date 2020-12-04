package state
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import source.StationLog
/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/3 14:30
*   @Description : 
*
*/
object TestKeyedState1 {
  /**
   * 第一种方法的实现
   * 统计每个手机的呼叫时间间隔，单位是毫秒
   */
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    //读取数据源
    var filePath =getClass.getResource("/station.log").getPath
    val stream: DataStream[StationLog] = streamEnv.readTextFile(filePath)
      .map(line=>{
        var arr=line.split(",")
        new StationLog(arr(0).trim,arr(1).trim,arr(2).trim,arr(3).trim,arr(4).trim.toLong,arr(5).trim.toLong)
      })
    stream.keyBy(_.callOut).flatMap(new CallIntervalFunction).print()
    streamEnv.execute()
  }

  // Return type is tuple2
  class CallIntervalFunction extends RichFlatMapFunction[StationLog, (String, Long)]{
    private var preCallTimeState:ValueState[Long]=_

    override def open(parameters: Configuration): Unit = {
      preCallTimeState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("pre", classOf[Long]))
    }
    override def flatMap(in: StationLog, collector: Collector[(String, Long)]): Unit = {
      //从状态中取得前一次呼叫的时间
      var preCallTime = preCallTimeState.value()
      if(preCallTime == null || preCallTime == 0){
        preCallTimeState.update(in.callTime)
      } else {
        //状态中有数据,则要计算时间间隔
        var interval = Math.abs(in.callTime - preCallTime)
        collector.collect((in.callOut, interval))
      }
    }
  }
}
