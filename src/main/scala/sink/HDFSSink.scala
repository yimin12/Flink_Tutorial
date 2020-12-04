package sink

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import source.{MyCustomerSource, StationLog}
import org.apache.flink.streaming.api.scala._

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/3 12:37
*   @Description : 
*
*/
object HDFSSink {

  //需求：把自定义的Source作为数据源，把基站日志数据写入HDFS并且每隔两秒钟生成一个文件
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomerSource)
    // set bucket in each hour, and set the rolling stragegy
    val rolling:DefaultRollingPolicy[StationLog, String] = DefaultRollingPolicy.create().withInactivityInterval(2000) // inactive bucket time
      .withRolloverInterval(2000) // create a file in every two second
      .build()

    // create sink of hdfs
    val hdfsSink: StreamingFileSink[StationLog] = StreamingFileSink.forRowFormat[StationLog](
      new Path("hdfs://hadoop101:9000/MySink001/"),
      new SimpleStringEncoder[StationLog]("UTF-8"))
        .withRollingPolicy(rolling).withBucketCheckInterval(1000).build()
    stream.addSink(hdfsSink)
    streamEnv.execute()
  }
}
