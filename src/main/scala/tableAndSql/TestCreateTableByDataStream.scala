package tableAndSql

import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row
import source.StationLog

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/3 15:14
*   @Description : 
*
*/
object TestCreateTableByDataStream {

  def main(args: Array[String]): Unit = {
    // use flink to generate TableEnvironment
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv, settings)

    //两个隐式转换
    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api.scala._

    val stream: DataStream[StationLog] = streamEnv.socketTextStream("hadoop101",8888).map(line => {
      var arr = line.split(",")
      new StationLog(arr(0).trim,arr(1).trim,arr(2).trim,arr(3).trim,arr(4).trim.toLong,arr(5).trim.toLong)
    })

    // create a table with no return type
    //    tableEnv.registerDataStream("t_table2",stream)

    //    tableEnv.sqlQuery("select * from t_table2").printSchema()
    // can use sql API
    // print the table structure or use Tble API
    //    val table: Table = tableEnv.scan("t_table2")
    //    table.printSchema() //打印表结构

    // change the field of table
    //    val table: Table = tableEnv.fromDataStream(stream,'id,'call_out,'call_in) //把后面的字段补全
    //    table.printSchema()
    val table: Table = tableEnv.fromDataStream(stream)
    val result: Table = table.groupBy('sid).select('sid,'sid.count as 'log_count)
    tableEnv.toRetractStream[Row](result).filter(_._1==true).print()
    streamEnv.execute()
  }
}
