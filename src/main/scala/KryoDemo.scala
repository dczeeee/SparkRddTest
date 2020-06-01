import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object KryoDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Kryo")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[MySearcher]))

    val sc = new SparkContext(conf)
    val rdd1: RDD[String] = sc.parallelize(Array("hadoop yarn", "hadoop hdfs", "c"))
    val rdd2: RDD[String] = MySearcher("hadoop").getMathcRddByQuery(rdd1)
    rdd2.foreach(println)


  }

  case class MySearcher(val query: String) {
    def getMathcRddByQuery(rdd: RDD[String]): RDD[String] = {
      rdd.filter(x => x.contains(query))
    }
  }
}
