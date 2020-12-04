package transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import source.{MyCustomerSource, StationLog}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/2 22:26
*   @Description : 
*
*/
object TestSplitAndSelect {

  //需求：从自定义的数据源中读取基站通话日志，把通话成功的和通话失败的分离出来
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomerSource) // read data
    // cut
    val splitStream: SplitStream[StationLog] = stream.split( //流并没有真正切割
      log => {
        if (log.callType.equals("success")) {
          Seq("Success")
        }else{
          Seq("NO Success")
        }
      }
    )

    var stream1 = splitStream.select("Success")
    var stream2 = splitStream.select("No success")
    stream.print("Original Data")
    stream1.print("Connected successfully")
    stream2.print("Connected failed")

    streamEnv.execute()
  }


}
