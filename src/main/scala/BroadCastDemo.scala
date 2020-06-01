import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BroadCastDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("BroadCast").setMaster("local[*]")
    val sc = new SparkContext(conf)

    /**
     * 广播变量在每个节点上保存一个只读的变量的缓存，而不用给每个task来传送一个copy
     */
    val rdd: RDD[String] = sc.parallelize(Array("a", "b"))
    val broadArr: Broadcast[Array[Int]] = sc.broadcast(Array(1, 2))
    rdd.foreach(x => {
      val value: Array[Int] = broadArr.value
      println(value.toList)
    })

  }
}
