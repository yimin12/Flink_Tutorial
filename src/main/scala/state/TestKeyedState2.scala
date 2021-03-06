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
*   @Date : 2020/12/3 14:38
*   @Description : 
*
*/
/**
 * 第二种方法的实现
 * 统计每个手机的呼叫时间间隔，单位是毫秒
 */
object TestKeyedState2 {

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
    stream.keyBy(_.callOut).mapWithState[(String, Long), StationLog]{
      case (in:StationLog,None) => ((in.callOut,0),Some(in)) //状态中没有值 是第一次呼叫
      case (in:StationLog,pre:Some[StationLog]) => {//状态中有值，是第二次呼叫
        var interval = Math.abs(in.callTime - pre.get.callTime)
        ((in.callOut,interval),Some(in))
      }
    }.filter(_._2 != 0).print()

    streamEnv.execute()
  }
}
