package transformation

import java.sql.{Connection, PreparedStatement}
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import source.StationLog

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/2 21:35
*   @Description : 
*
*/
object TestRichFunctionClass {

  // Mapping the telephone number to the real name, and save the name to mysql
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    //读取数据源
    var filePath =getClass.getResource("/station.log").getPath
    val stream: DataStream[StationLog] = streamEnv.readTextFile(filePath).map(line=>{
      var arr = line.split(",")
      new StationLog(arr(0).trim,arr(1).trim,arr(2).trim,arr(3).trim,arr(4).trim.toLong,arr(5).trim.toLong)
    })
    val result:DataStream[StationLog] = stream.filter(_.callType.equals("success")).map(new MyRichMapFunction)
    result.print()
    streamEnv.execute()
  }

  // Define a rich function
  class MyRichMapFunction extends RichMapFunction[StationLog,StationLog]{
    var conn:Connection=_
    var pst:PreparedStatement=_
    override def open(parameters: Configuration): Unit = {
      conn =DriverManager.getConnection("jdbc:mysql://localhost/test","root","123123")
      pst =conn.prepareStatement("select name from t_phone where phone_number=?")
    }

    override def close(): Unit = {
      pst.close()
      conn.close()
    }

    override def map(value: StationLog): StationLog = {
      println(getRuntimeContext.getTaskNameWithSubtasks)
      //查询主叫号码对应的姓名
      pst.setString(1,value.callOut)
      val result: ResultSet = pst.executeQuery()
      if(result.next()){
        value.callOut=result.getString(1)
      }
      //查询被叫号码对应的姓名
      pst.setString(1,value.callIn)
      val result2: ResultSet = pst.executeQuery()
      if(result2.next()){
        value.callIn=result2.getString(1)
      }
      value
    }
  }
}
