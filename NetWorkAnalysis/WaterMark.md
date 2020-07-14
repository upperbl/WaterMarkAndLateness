## Flink WaterMark移动以及迟到数据分析

### Demo代码

**将以此代码进行实验分析，不想看代码直接看下面章节即可**

```
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

import org.apache.flink.api.common.functions.AggregateFunction

class UserAgg extends AggregateFunction[UserBehavior,Long,Long]{
  override def createAccumulator(): Long = 0l

  override def add(in: UserBehavior, acc: Long): Long = acc+1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.util.Collector
import scala.collection.mutable.ListBuffer
class UserProcessFunction extends KeyedProcessFunction[Long,UserViewCount,String]{
  lazy val liststate: MapState[Long,Long] = getRuntimeContext.getMapState[Long,Long](new MapStateDescriptor[Long,Long]("item-list", classOf[Long],classOf[Long]))
  override def processElement(i: UserViewCount, context: KeyedProcessFunction[Long, UserViewCount, String]#Context, collector: Collector[String]): Unit = {
    liststate.put(i.itemId,i.cnt)

    context.timerService().registerEventTimeTimer(i.window+1)
    context.timerService().registerEventTimeTimer(i.window+3*1000l)
  }
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UserViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    if(timestamp==ctx.getCurrentKey+3*1000l){
      //等待窗口彻底关闭后再清除状态，等待延迟数据可以更新状态
      print("clear state==================")
      liststate.clear()
      return
    }
    val iter = liststate.iterator()
    val lst=ListBuffer[UserViewCount]()
    val builder = new StringBuilder()
    while (iter.hasNext){
      val value = iter.next()
      lst.append(UserViewCount(value.getKey,ctx.getCurrentKey,value.getValue))
      val counts = lst.sortBy(_.cnt)(Ordering.Long.reverse).take(3)
      for(i<-counts) {
        builder.append(i.itemId).append(" ").append(TimeGet.getInstance.getDate(i.window)).append(" ").append(i.cnt).append("\n")
      }
    }
    out.collect(builder.toString())
  }
}

import com.protocol.UserViewCount
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class UserWindowFun extends WindowFunction[Long,UserViewCount,Long,TimeWindow]{

  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[UserViewCount]): Unit = {
    print(TimeGet.getInstance.getTime(window.getStart) +"--------------------------------------------"+TimeGet.getInstance.getTime(window.getEnd)+"\n")
    out.collect(UserViewCount(key,window.getEnd,input.iterator.next()))
  }
}
import java.sql.Date
import java.text.SimpleDateFormat
import com.tools.TimeGet.simpleDateFormat

class TimeGet private() {
  def getTime(time:Long):String={

    val date1 = new Date(time)
    simpleDateFormat.format(date1)
  }
  def getDate(timeStamp:Long):String={
    simpleDateFormat.format(timeStamp)
  }
}
object TimeGet{
  val simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

  private var s:TimeGet=null

  def getInstance={
    if (s==null){
      s=new TimeGet()
    }
    s
  }
}
```

### 设置参数

设置事件事件

```scala
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
```

watermark延迟**1s**

```scala
new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(1)
```

设置窗口size为**10s**，滑步长为**2s**

```scala
timeWindow(Time.seconds(10), Time.seconds(2)
```

如果数据迟到**3S**则直接丢弃

```
allowedLateness(Time.seconds(3)
```

### 数据格式

UserBehavior(543462,1715,1464116,pv,**1511658000000**)最后一个字段提取成EventTime

### 秒级场景预演

根据上面设置的参数以及数据格式，分别逐秒上传数据

#### 上传第1条数据

543462,1715,1464116,pv,1511658000（**EventTime**: 2017-11-26 **09:00:00**）

建立第一个窗口【**08:59:52**-**09:00:02**）

建立第1个滑步[09:00:00-09:00:02)

此时窗口未关闭，未触发窗口相关计算操作

#### **上传第2条数据**

543462,1715,1464116,pv,1511658001（**EventTime**: 2017-11-26 09:00:01）

处于第1个滑步【**09:00:00-09:00:02)**

处于第1个窗口【**08:59:52**-**09:00:02**）

未触发窗口相关计算操作

#### **上传第3条数据**

543462,1715,1464116,pv,1511658002（**EventTime**: 2017-11-26 09:00:02）

建立第2个窗口【**08:59:54-**09:00:04）

建立第2个滑步[09:00:02-09:00:04)

watermark设置的为延迟一秒所以此时第一个滑步还不会结束 ，也就是说第一个滑步会在**09:00:03**结束

未触发窗口相关计算操作

#### **上传第4条数据**

543462,1715,1464116,pv,1511658003（**EventTime**: 2017-11-26 09:00:03）

处于第2个窗口【**08:59:54**-**09:00:04）

处于第2个滑步[09:00:02-09:00:04)

watermark设置的为延迟一秒，已经达到，此时会触发基于窗口的agg操作

```
agg> UserViewCount(1715,1511658002000,2)
```

processFunction里面设置的触发器Ontimer是window.end+1，将在09:00:04到达的时候触发processFunction里面的状态计算

第1个滑步[**09:00:00-09:00:02)** ，触发agg计算

#### **上传第5条数据**

543462,1715,1464116,pv,1511658004（**EventTime**: 2017-11-26 09:00:04）

时间到达注册定时器的阈值，触发第一个窗口【**09:00:52**-**09:00:02**）的计算，并输出结果，计算逻辑是按窗口和第一列字段分区算count TopN，因为我们第一列一直为一个，所以此窗口输出结果为2.

```
topN> 1715 1511658002000 2
```

注意此时不会关闭第一个窗口，因为有**allowedLateness(Time.seconds(3)）**，3s之后这个窗口才会关闭，3s之内再来这个滑步区间的值还会更新结果值

建立第3个窗口【**08:59:56-**09:00:06）

建立第3个滑步[09:00:04-09:00:06)

watermark设置的为延迟一秒所以此时第2个滑步还不会结束 

#### **上传第6条数据**

543462,1715,1464116,pv,1511658005

...

#### **上传第7条数据**

543462,1715,1464116,pv,1511658006

第一个窗口彻底关闭，迟到的极限时间为09:00:02+1+3=**09:00:06**，再等一秒触发窗口创新计算

第2个窗口【**08:59:54-**09:00:04）触发计算

```
topN> 1715 1511658004000 4
```



#### 上传第8条数据

543462,1715,1464116,pv,1511658000（**EventTime**: 2017-11-26 09:00:01）

该数据为延迟数据，但是小于allowedLateness(Time.seconds(3)）（**如果某条数据无法算作当前所有开着的窗口中的一条数据，则该数据将被标记为late数据，并输出到late流**）

延迟时间大于watermark延迟，小于allowedLateness(Time.seconds(3)），这个时间段的数据不会被输出到迟到数据流

此时第1个窗口【**08:59:52**-**09:00:02**）已经在 **09:00:06**彻底关闭（**09:00:03**的时候触发窗口TOPN计算，但是算完之后状态没有清空，窗口也没有关闭，还在等待延迟数据，不过会在3s之后即**09:00:06**彻底关闭）

此时第2个窗口【**08:59:54-09:00:04**）将在 **09:00:08**关闭并在1ms之后出发计算，而这条数据（**EventTime**: 2017-11-26 09:00:01）属于第2个窗口



#### **上传第9条数据**

该数据位延迟数据

543462,1715,1464116,pv,1511658008（**EventTime**: 2017-11-26 09:00:08）

此时迟到数据重新计算第二个窗口【**08:59:54-09:00:04**）

同时第三个窗口【**08:59:56-09:00:06**）触发第一次TOPN计算

即输出结果如下

```
topN1> 1715 2017-11-26 09:00:04 5
topN1> 1715 2017-11-26 09:00:06 7
```

#### **上传第10条数据**

543462,1715,1464116,pv,1511657994

此数据位迟到数据，何时为迟到数据

**如果某条数据无法算作当前所有开着的窗口中的一条数据，则该数据将被标记为late数据，并输出到late迟到流**

当输入上述第9条数据的时候，EventTime已经到了**09:00:08**，此时只有EventTime-1（watermark）-3（allowedLateness）-10（windowsize）=8:59:54

也就是说此时再来一条小于8:59:54的数据才会被当做迟到数据，丢进迟到流

```
late> UserBehavior(543462,1715,1464116,pv,1511657994000)
```

#### **上传第11条数据**

543462,1715,1464116,pv,1511658014

此条数据距离第9条数据差了6s，此时他会把中间所有未关闭（windowEnd4秒内的不会关闭，windowEnd秒以前的窗口已经彻底关闭）的切触发计算窗口进行计算

输出

```
topN1> 1715 2017-11-26 09:00:10 9

topN1> 1715 2017-11-26 09:00:12 7
```

