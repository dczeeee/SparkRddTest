import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

object AccumulatorDemo {
  def main(args: Array[String]): Unit = {

    /**
     * accumulotor：先分区内累加，再分区间累加
     */
    val conf = new SparkConf().setAppName("Accumulator").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5), 2)

    val acc: LongAccumulator = sc.longAccumulator("acc")
    rdd1.foreach(x => {
      acc.add(1)
      println("rdd: " + acc.value)
    })
    println("-----")
    println("main: "+ acc.count)

    //注册累加器
    val myacc = new MyAccumulator
    sc.register(myacc)

    rdd1.foreach(x => {
      myacc.add(1)
      println("rdd: " + myacc.value)
    })
    println("-----")
    println("main: " + myacc.value)

  }


}
/**
 * 自定义累加器
 *
 */
class MyAccumulator extends AccumulatorV2[Int, Int] {
  var sum: Int = 0

  //判断累加的值是不是空
  override def isZero: Boolean = sum == 0

  //如何把累加器copy到Executor
  override def copy(): AccumulatorV2[Int, Int] = {
    val accumulator = new MyAccumulator
    accumulator.sum = sum
    accumulator
  }

  //重置值
  override def reset(): Unit = {
    sum = 0
  }

  //分区内的累加
  override def add(v: Int): Unit = {
    sum += v
  }

  //分区间的累加，累加器最终的值
  override def merge(other: AccumulatorV2[Int, Int]): Unit = {
    other match {
      case o: MyAccumulator => this.sum += o.sum
      case _ =>
    }
  }

  override def value: Int = this.sum
}