package transformation

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import source.StationLog

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/2 16:50
*   @Description : 
*
*/
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestFunctionClass {

  def main(args: Array[String]): Unit = {
    //计算出每个通话成功的日志中呼叫起始和结束时间,并且按照指定的时间格式
    //数据源来自本地文件
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    // read data source
    var filePath = getClass.getResource("/station.log").getPath
    val stream: DataStream[StationLog] = streamEnv.readTextFile(filePath).map(line => {
      var arr = line.split(",")
      new StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
    })
    val format= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val result: DataStream[String] = stream.filter(_.callType.equals("success")).map(new MyMapFunction(format))
    result.print()
    streamEnv.execute()
  }

  class MyMapFunction(format:SimpleDateFormat) extends MapFunction[StationLog,String]{
    override def map(value: StationLog): String = {
      var startTime=value.callTime;
      var endTime= startTime + value.duration*1000
      "主叫号码："+value.callOut+",被叫号码:"+value.callIn+",呼叫起始时间:"+format.format(new Date(startTime))+",呼叫结束时间:"+format.format(new Date(endTime))
    }
  }
}
