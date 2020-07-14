import java.sql.Date
import java.text.SimpleDateFormat

import com.fun.{UserAgg, UserProcessFunction, UserWindowFun}
import com.protocol.{UserBehavior, UserViewCount}
import com.tools.TimeGet
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
object UserBehaviorAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    ()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val dataStream = env.socketTextStream("127.0.0.1",9999).filter(data=>{
      data.split(",").size==5
    }).map(data => {
      val dataArray = data.split(",")
      println(TimeGet.getInstance.getTime(dataArray(4).trim.toLong * 1000))
      UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong*1000)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(1)) {
      override def extractTimestamp(t: UserBehavior): Long = t.timestamp
    }).filter(user=>{
      user.behavior == "pv"
    })
    val tag = new OutputTag[UserBehavior]("latedata")
    val aggresult:DataStream[UserViewCount] = dataStream.keyBy(_.itemId).timeWindow(Time.seconds(10), Time.seconds(2)).allowedLateness(Time.seconds(3))
      .sideOutputLateData(tag).aggregate(new UserAgg(), new UserWindowFun())
    val result = aggresult.keyBy(_.window).process(new UserProcessFunction())
    dataStream.print("data")
    val lateDate = aggresult.getSideOutput(tag)
    lateDate.print("late")
    aggresult.print("agg")
    result.print("topN1")
    env.execute("test")

  }
}
