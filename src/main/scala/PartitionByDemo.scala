import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object PartitionByDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PartitionBy").setMaster("local[*]")
    val sc = new SparkContext(conf)

    /*
     * 大多数Spark算子都可以用在任意类型的RDD上，但是有一些比较特殊的操作只能用在key-value类型的RDD上
     */

    val rdd1: RDD[String] = sc.parallelize(Array("hello", "hadooop", "hello", "spark"), 1)
    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))

    println(rdd2.partitions.length) //1
    println(rdd2.partitioner) //None

    /**
     * 使用HashPartitioner分区器
     */
    val rdd3: RDD[(String, Int)] = rdd2.partitionBy(new HashPartitioner(2))
    println(rdd3.partitions.length) //2
    println(rdd3.partitioner) //Some(org.apache.spark.HashPartitioner@2)

    val result = rdd3.mapPartitionsWithIndex((index, it) => {
      it.map(x => (index, (x._1, x._2)))
    })

    result.foreach(println)

    /**
     * 自定义分区器
     */
    val rdd4: RDD[(String, Int)] = rdd2.partitionBy(new MyPartitioner(2))
    println(rdd4.partitions.length)
    println(rdd4.partitioner)
    val result1 = rdd4.mapPartitionsWithIndex((index, it) => {
      it.map(x => (index, (x._1, x._2)))
    })
    result1.foreach(println)

  }

  class MyPartitioner(num: Int) extends Partitioner {
    override def numPartitions: Int = num

    override def getPartition(key: Any): Int = {
      System.identityHashCode(key) % num.abs
    }
  }
}
