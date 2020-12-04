package state

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/3 13:47
*   @Description : 
*
*/
object TestCheckPointByHDFS {

  //使用WordCount案例来测试一下HDFS的状态后端，先运行一段时间Job，然后cansol，在重新启动，看看状态是否是连续的
  def main(args: Array[String]): Unit = {
    //1
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2. turn on the check point configuration
    streamEnv.enableCheckpointing(5000) // set check point in every 5 secs
    streamEnv.setStateBackend(new FsStateBackend("hdfs://hadoop101:9000/checkpoint/cp1")) // set the address
    streamEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    streamEnv.getCheckpointConfig.setCheckpointTimeout(5000)
    streamEnv.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    streamEnv.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION) // check the saved data and terminate the job

    streamEnv.setParallelism(1)
    import org.apache.flink.streaming.api.scala._
    val stream: DataStream[String] = streamEnv.socketTextStream("hadoop101",8888)
    val result:DataStream[(String, Int)] = stream.flatMap(_.split(" ")).map((_, 1)).setParallelism(2).keyBy(0).sum(1).setParallelism(2)
    result.print("Result").setParallelism(1)
    streamEnv.execute("wordCount")
  }
}
