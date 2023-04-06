package com.pta.HotItems

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import scala.collection.mutable.ListBuffer

//输入数据
case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long )
//输出数据
case class ItemViewCount( itemId: Long, windowEnd: Long, count: Long )

object HotItems {
  def main(args: Array[String]): Unit = {

    //val properties = new Properties()

    //flink的流执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设定Time类型为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //并发为1
    env.setParallelism(1)

    //获得数据源的文件
    val stream = env
      .readTextFile("E:\\Flink项目\\UserBehaviorAnalysis\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

      //获得文件中的每一行记录
      .map(line => {
        val linearray = line.split(",")
        UserBehavior( linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3), linearray(4).toLong )
      })

      //指定时间戳
      .assignAscendingTimestamps(_.timestamp * 1000)
      //进行过滤，只获得pv行为的数据
      .filter(_.behavior == "pv")
      //按照商品id进行分流
      .keyBy("itemId")
      //滑动窗口
      .timeWindow(Time.hours(1), Time.minutes(5))
      //预聚合，第一个参数：求和，第二个参数：每个商品在每个窗口的点击量
      .aggregate( new CountAgg(), new WindowResultFunction() )
      //不同商品但同一窗口的数据在一起
      .keyBy("windowEnd")
      //在自定义函数中进行业务逻辑处理
      .process( new TopNHotItems(3))
      .print()


    env.execute("Hot Items Job")
  }


  //统计出总数
  class CountAgg extends AggregateFunction[UserBehavior, Long, Long]{
    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

    override def createAccumulator(): Long = 0L

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }


  //获得某个商品在一个窗口中的总数
  class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow]{
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
      val count = input.iterator.next()
      out.collect(ItemViewCount(itemId, window.getEnd, count))
    }
  }


  //求某个窗口中前N名的热门点击商品
  class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String]{


    //ListState用于保存状态
    private var itemState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)

      val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemState", classOf[ItemViewCount])
      //创建ListState对象
      itemState = getRuntimeContext.getListState(itemStateDesc)
    }

    //注册定时器
    override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
      //添加本窗口中的所有数据
      itemState.add(i)

      //windowEnd+1ms作为关闭窗口的触发时间，1ms
      context.timerService.registerEventTimeTimer( i.windowEnd + 1 )
    }


    //到了windowEnd+1时间后，执行onTimer方法
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

      val allItems: ListBuffer[ItemViewCount] = ListBuffer()
      import scala.collection.JavaConversions._

      //将ListStae对象的数据存入到ListBuffer对象
      for(item <- itemState.get){
        allItems += item
      }

      //清除状态中的数据
      itemState.clear()

      //按count进行降序排序，获得前3名的数据
      val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

      //格式化，打印
      val result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

      for( i <- sortedItems.indices ){
        val currentItem: ItemViewCount = sortedItems(i)

        result.append("No").append(i+1).append(":")
          .append("  商品ID=").append(currentItem.itemId)
          .append("  浏览量=").append(currentItem.count).append("\n")
      }
      result.append("====================================\n\n")

      //控制输出频率，模拟实时滚动
      Thread.sleep(1000)
      out.collect(result.toString)
    }
  }
}
