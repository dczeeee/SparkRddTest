import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RddAndRdd {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RddAndRdd").setMaster("local[*]")
    val sc = new SparkContext(conf)

    /**
     * union：并集
     * intersection：交集
     * subtract：差集
     * cartesian: 笛卡尔积
     * zip： 拉链，两rdd元素数量相同
     */

    val rdd1: RDD[Int] = sc.parallelize(1.to(5), 1)
    val rdd2: RDD[Int] = sc.parallelize(4.to(8), 1)

    println("并集union")
    rdd1.union(rdd2).collect().foreach(x => print(x + "\t"))
    println()
    println("交集intersection")
    rdd1.intersection(rdd2).collect().foreach(x => print(x + "\t"))
    println()
    println("差集subtract")
    rdd1.subtract(rdd2).collect().foreach(x => print(x + "\t"))
    println()
    println("笛卡尔积cartesian")
    rdd1.cartesian(rdd2).collect().foreach(x => print(x + "\t"))
    println()
    println("拉链zip")
    rdd1.zip(rdd2).collect().foreach(x => print(x + "\t"))
    println()

  }
}
