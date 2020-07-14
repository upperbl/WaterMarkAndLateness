package com.fun

import com.protocol.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction

class UserAgg extends AggregateFunction[UserBehavior,Long,Long]{
  override def createAccumulator(): Long = 0l

  override def add(in: UserBehavior, acc: Long): Long = acc+1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}
