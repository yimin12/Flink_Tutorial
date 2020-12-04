package sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import source.{MyCustomerSource, StationLog}


/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/2 23:49
*   @Description : 
*
*/
object CustomerJdbcSink {

  //需求：随机生成StationLog对象，写入Mysql数据库的表（t_station_log）中
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    streamEnv.setParallelism(1)
    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomerSource)
    stream.addSink(new MyCustomerJdbcSink)
    streamEnv.execute("jdbcSink")
  }

  class MyCustomerJdbcSink extends RichSinkFunction[StationLog]{
    var conn :Connection=_
    var pst :PreparedStatement=_

    //把StationLog对象写入Mysql表中，每写入一条执行一次
    override def invoke(value: StationLog, context: SinkFunction.Context[_]): Unit = {
      pst.setString(1,value.sid)
      pst.setString(2,value.callOut)
      pst.setString(3,value.callIn)
      pst.setString(4,value.callType)
      pst.setLong(5,value.callTime)
      pst.setLong(6,value.duration)
      pst.executeUpdate()
    }
    //Sink初始化的时候调用一次，一个并行度初始化一次
    //创建连接对象，和Statement对象
    override def open(parameters: Configuration): Unit = {
      conn =DriverManager.getConnection("jdbc:mysql://localhost/test","root","123123")
      pst =conn.prepareStatement("insert into t_station_log (sid,call_out,call_in,call_type,call_time,duration) values (?,?,?,?,?,?)")
    }

    override def close(): Unit = {
      pst.close()
      conn.close()
    }
  }
}
