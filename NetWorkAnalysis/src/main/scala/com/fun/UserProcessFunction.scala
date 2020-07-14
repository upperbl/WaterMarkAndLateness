package com.fun
import com.protocol.UserViewCount
import com.tools.TimeGet
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
