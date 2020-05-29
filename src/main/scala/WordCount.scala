import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val tuples: Array[(String,Int)] = sc.textFile(ClassLoader.getSystemResource("word.csv").getPath)
      .flatMap(_.split(","))
      .map((_,1))
      .reduceByKey(_+_)
      .collect()

    tuples.foreach(println)
  }
}
