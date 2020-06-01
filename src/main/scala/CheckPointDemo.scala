import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CheckPointDemo {
  def main(args: Array[String]): Unit = {
    //设置当前用户
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf = new SparkConf().setAppName("checkpoint").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //设置checkpoint目录
    sc.setCheckpointDir("hdfs://hcs0115:8020/test")
    val rdd1: RDD[String] = sc.parallelize(Array("abc"))
    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))

    /**
     * 标记RDD2的checkpoint
     * RDD2会被保存到文件中，并且会切断到父RDD的引用，该持久化操作，必须在job运行之前调用
     * 如果不进行持久化操作，那么在保存到文件的时候需要重新计算
     **/
    rdd2.cache()
    rdd2.collect.foreach(x => print(x + "\t"))
    rdd2.collect.foreach(x => print(x + "\t"))
  }
}
