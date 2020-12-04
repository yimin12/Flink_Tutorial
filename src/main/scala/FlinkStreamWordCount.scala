import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Word Count in streaming of flink
 */
object FlinkStreamWordCount {

  def main(args: Array[String]): Unit = {
    //1、初始化Flink流计算的环境
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    //2、导入隐式转换
    import org.apache.flink.streaming.api.scala._
    val stream: DataStream[String] = streamEnv.socketTextStream("mynode5",8888)

    val result: DataStream[(String, Int)] = stream.flatMap(_.split(" ")).map((_, 1)).setParallelism(2)
      .keyBy(0)//分组算子  : 0 或者 1 代表下标。前面的DataStream[二元组] , 0代表单词 ，1代表单词出现的次数
      .sum(1).setParallelism(2)

    //3. sink
    result.print("Result").setParallelism(1)
    streamEnv.execute("wordCount")
  }
}
