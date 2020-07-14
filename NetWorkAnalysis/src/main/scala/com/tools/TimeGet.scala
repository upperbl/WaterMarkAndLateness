package com.tools

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
