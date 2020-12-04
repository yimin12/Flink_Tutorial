import java.net.URL

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/2 14:56
*   @Description : 
*
*/
object BatchWordCount {

  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val dataPath: URL = getClass.getResource("/wc.txt")
    // read the data
    val data: DataSet[String] = env.readTextFile(dataPath.getPath)
    data.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .print()
  }
}
