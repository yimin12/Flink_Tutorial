package source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/2 18:08
*   @Description : 
*
*/
object HDFSFileSource {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    import org.apache.flink.streaming.api.scala._
    val stream: DataStream[String] = streamEnv.readTextFile("hdfs://hadoop101:9000/wc.txt")
    val result: DataStream[(String, Int)] = stream.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)

    result.print()
    streamEnv.execute("wordCount")
  }

}
