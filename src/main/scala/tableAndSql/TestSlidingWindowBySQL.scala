package tableAndSql

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import source.StationLog

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/3 15:45
*   @Description : 
*
*/
object TestSlidingWindowBySQL {

  //每隔5秒钟统计，每个基站的通话数量,假设数据是乱序。最多延迟3秒,需要水位线
  def main(args: Array[String]): Unit = {
    // init the streaming context adn create TableEnvironment
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.setParallelism(1)
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv, settings)
    //两个隐式转换
    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api.scala._

    val stream: DataStream[StationLog] = streamEnv.socketTextStream("hadoop101",8888)
      .map(line=>{
        var arr=line.split(",")
        new StationLog(arr(0).trim,arr(1).trim,arr(2).trim,arr(3).trim,arr(4).trim.toLong,arr(5).trim.toLong)
      })
      //引入Watermark，让窗口延迟触发
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[StationLog](Time.seconds(3)) {
        override def extractTimestamp(element: StationLog) = {
          element.callTime
        }
      })
    tableEnv.registerDataStream("t_station_log", stream,'sid,'callOut,'callInt,'callType,'callTime.rowtime,'duration)
    val result: Table = tableEnv.sqlQuery("select sid, hop_start(callTime,interval '5' second,interval '10' second),hop_end(callTime,interval '5' second,interval '10' second)" +
      ",sum(duration) " +
      "from t_station_log " +
      "where callType='success' " +
      "group by hop(callTime,interval '5' second,interval '10' second),sid")
    tableEnv.toRetractStream[Row](result).filter(_._1 == true).print()
    tableEnv.execute("sql")
  }
}
