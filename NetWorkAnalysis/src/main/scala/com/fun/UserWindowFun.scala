package com.fun

import com.protocol.UserViewCount
import com.tools.TimeGet
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class UserWindowFun extends WindowFunction[Long,UserViewCount,Long,TimeWindow]{

  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[UserViewCount]): Unit = {
    out.collect(UserViewCount(key,window.getEnd,input.iterator.next()))
  }
}
