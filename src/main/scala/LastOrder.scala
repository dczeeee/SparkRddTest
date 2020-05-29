import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

object LastOrder {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LastOrder").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.textFile("src/main/resources/orders.txt")
    val orders = data.map(line => {
      val splits = line.split(",")
      (splits(0), (splits(1).toInt, splits(2)))
    })
      .groupByKey()
      .map(x => {
        val userid = x._1
        val amtDate = x._2.toArray.sortBy(_._2).reverse.take(1)
        (userid, amtDate)
      })
      .foreach(x => {
        print(x._1)
        x._2.foreach(println)
      })

  }
}
