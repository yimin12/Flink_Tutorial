package tableAndSql

import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sources.CsvTableSource

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/3 15:31
*   @Description : 
*
*/
object TestCreateTableByFile {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv, settings)
    val tableSouce = new CsvTableSource("/station.log",Array[String]("f1","f2","f3","f4","f5","f6"),Array(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.LONG))

    // create a table
    tableEnv.registerTableSource("t_station_log", tableSouce)
    val table: Table = tableEnv.scan("t_station_log")
    table.printSchema()
  }
}
