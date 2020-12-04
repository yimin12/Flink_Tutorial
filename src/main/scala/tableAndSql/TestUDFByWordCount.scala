package tableAndSql

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, Types}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/3 18:56
*   @Description : 
*
*/
object TestUDFByWordCount {
  //使用tableAPI实现WordCount
  def main(args: Array[String]): Unit = {
    //使用Flink原生的代码创建TableEnvironment
    //先初始化流计算的上下文
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv,settings)

    //两个隐式转换
    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api.scala._

    val stream: DataStream[String] = streamEnv.socketTextStream("hadoop101", 8888)
    val table: Table = tableEnv.fromDataStream(stream,'line)
  }

  class MyFlatMapFunction extends TableFunction[Row]{
    override def getResultType: TypeInformation[Row] = Types.ROW(Types.STRING(),Types.INT())
    def eval(str:String):Unit={
      str.trim.split(" ").foreach(word=>{
        var row = new Row(2)
        row.setField(0, word)
        row.setField(1,1)
        collect(row)
      })
    }
  }
}
