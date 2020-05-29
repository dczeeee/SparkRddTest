import org.apache.spark.{SparkConf, SparkContext}

object FlatMapPartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("map flatMap mapPartitions").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Seq(("A", 1), ("A", 2), ("A", 3), ("B", 1), ("B", 2), ("C", 1), ("C", 2), ("C", 3)), 3)

    val rdd1 = rdd.map(_._2*2).foreach(println(_))
    val rdd2 = rdd.flatMap(_._1.split(" ")).foreach(println(_))
    val rdd3 = rdd.mapPartitions(_.filter(_._2>2)).foreach(println(_))

  }
}
