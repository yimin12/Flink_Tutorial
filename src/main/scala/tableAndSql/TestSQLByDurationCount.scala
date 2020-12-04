package tableAndSql
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row
import source.StationLog
/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/3 15:59
*   @Description : 
*
*/
object TestSQLByDurationCount {

  /**
   * 统计每个基站中，通话成功的通话总时长
   */
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api.scala._
    streamEnv.setParallelism(1)
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv,settings)

    // can use tableAPI and sql together
    val stream: DataStream[StationLog] = streamEnv.readTextFile(getClass.getResource("/station.log").getPath).map(line=>{
      val arr: Array[String] = line.split(",")
      new StationLog(arr(0).trim,arr(1).trim,arr(2).trim,arr(3).trim,arr(4).trim.toLong,arr(5).trim.toLong)
    })
    val table: Table = tableEnv.fromDataStream(stream)
    val resut: Table = tableEnv.sqlQuery(s"select sid, sum(duration) as d_c from &table where callType='success' group by sid")
    tableEnv.toRetractStream[Row](resut).filter(_._1 == true).print()
    tableEnv.execute("sql")
  }
}
