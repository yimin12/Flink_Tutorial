package state
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/3 15:08
*   @Description : 
*
*/
object TestSavePoint {

  def main(args: Array[String]): Unit = {
    //1、初始化Flink流计算的环境
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //修改并行度
    streamEnv.setParallelism(1) //默认所有算子的并行度为1
    //2、导入隐式转换
    import org.apache.flink.streaming.api.scala._
    //3、读取数据,读取sock流中的数据
    val stream: DataStream[String] = streamEnv.socketTextStream("hadoop101",8888) //DataStream ==> spark 中Dstream
      .uid("socket001")
    val result:DataStream[(String, Int)] = stream.flatMap(_.split(" ")).uid("flatmap001").map((_, 1)).setParallelism(2).uid("map001")
      .keyBy(0).sum(1).uid("sum001")
    result.print("Result").setParallelism(1)
    streamEnv.execute("wordCount")
  }
}
