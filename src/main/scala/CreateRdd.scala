import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object CreateRdd {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CreateRdd").setMaster("local[*]")
    val sc = new SparkContext(conf)

    /**
     * 1.从集合中创建Rdd
     * 通过parallelize方法传入序列得到RDD
     * 传入分区数为1，结果为12345678910
     * 传入分区数大于1，结果顺序不定，因为数据被打散在2个分区里
     */
    println("parallelize2rdd")
    val rdd1: RDD[Int] = sc.parallelize(1.to(10), 2)
    rdd1.foreach(x => print(x + "\t"))
    println()

    /**
     * 2.从外部存储中创建
     * 读取json文件
     */
    println("json2rdd")
    val rdd2: RDD[String] = sc.textFile(this.getClass.getClassLoader.getResource("word.json").getPath)
    val rdd3 = rdd2.map(JSON.parseFull(_))
    rdd3.foreach(println)
    println()

    /**
     * 3.读取object文件
     */
//    println("objectFile")
//    rdd1.saveAsObjectFile("hdfs://hcs0115:8020/tmp")
//    val rdd4 = sc.objectFile("hdfs://hcs0115:8020/tmp")
//    rdd4.foreach(println)
//    println()

    /**
     * 4.从别的rdd转换为新的rdd
     * map算子有多少元素就执行多少次，与分区无关，无shuffle
     */
    println("rdd2rdd")
    val mapRdd: RDD[Int] = rdd1.map(x=> {
      println("map执行")
      x * 2
    })
    val result: Array[Int] = mapRdd.collect()
    result.foreach(x => print(x + "\t"))
    println()

    /**
     * mapPartitions算子：每个分区内执行，有多少分区执行多少次，优于map
     */
    println("mapPartitions")
    val mapPartitionsRdd: RDD[Int] = rdd1.mapPartitions(x => {
      println("mapPartitions执行")
      x.map(_ * 2)
    })
    val mapPartitionsRddResult: Array[Int] = mapPartitionsRdd.collect()
    mapPartitionsRddResult.foreach(x => print(x + "\t"))
    println()

    /**
     * mapPartitionsWithIndex算子，一个分区内处理，几个分区就执行几次，返回带有分区号的结果集
     */
    println("mapPartitionsWithIndex")
    val mapPartitionsWithIndexRdd: RDD[(Int, Int)] = rdd1.mapPartitionsWithIndex((index, it) => {
      println("mapPartitionsWithIndex执行")
      it.map((index, _))
    })
    val mapPartitionsWithIndexRddResult: Array[(Int, Int)] = mapPartitionsWithIndexRdd.collect()
    mapPartitionsWithIndexRddResult.foreach(x => print(x + "\t"))
    println()

    /**
     * flatMap：扁平化，无shuffle
     */
    println("flatMap")
    val rdd5: RDD[(String, Int)] = sc.parallelize(Array(("A", 1), ("B", 2), ("C", 3)))
    val mapRdd1: RDD[String] = rdd5.map(x=> x._1 + x._2)
    val flatMapRdd: RDD[Char] = rdd5.flatMap(x=> x._1 + x._2)
    mapRdd1.foreach(println)
    flatMapRdd.foreach(println)
    println()

    /**
     * glom: 将每一个分区的元素合并成一个数组，形成新的RDD类型：RDD[Array[T]]，(不存在shuffle)
     */
    println("glom")
    val glomResult:  RDD[Array[Int]] = rdd1.glom()
    glomResult.foreach(x=>{
      println(x.mkString(","))
    })
    println()

    /**
     * groupBy：根据条件函数分组(存在shuffle)
     */
    println("groupBy")
    val groupByResult1: RDD[(Int, Iterable[Int])] = rdd1.groupBy(x=> x % 2)
    val groupByResult2: RDD[(Boolean, Iterable[Int])] = rdd1.groupBy(x=> x % 2 == 0)

    groupByResult1.foreach(println)
    groupByResult2.foreach(println)
    println()

    /**
     * filter：过滤，无shuffle
     */
    println("filter")
    val filterResult: RDD[Int] = rdd1.filter(x=> x % 2 == 0)
    filterResult.foreach(println)
    println()

    /**
     * sample（withReplacement,fraction,seed）抽样，常用在解决定位大key问题
     */
    println("sample")
    //1.无放回抽样，结果无重复
    val sampleResult1: RDD[Int] = rdd1.sample(false, 0.5)
    //2.有放回抽样，结果有重复
    val sampleResult2: RDD[Int] = rdd1.sample(true, 0.5)
    sampleResult1.foreach(x=>print(x+"\t"))
    println()
    sampleResult2.foreach(x=>print(x+"\t"))
    println()

    /**
     * distinct([numTasks])去重，参数表示任务数量，默认值和分区数保持一致(不存在shuffle)
     */
    println("distinct")
    val rdd6: RDD[Int] = sc.parallelize(Array(1,1,2,2,3,3,4,4,5,5))
    val distinctResult: RDD[Int] = rdd6.distinct(2)
    distinctResult.foreach(x=>print(x+"\t"))
    println()

    /**
     * coalesce(numPatitions)缩减，缩减分区到指定数量，用于大数据集过滤后，提高小数据集的执行效率，只能减不能加。(不存在shuffle)
     */
    println("coalesce")
    val rdd7: RDD[Int] = sc.parallelize(1.to(10), 5)
    val coalesceResult: RDD[Int] = rdd7.coalesce(2)
    println(coalesceResult.partitions.length)
    println()

    /**
     * repartition(numPatitions)更改分区，更改分区到指定数量，可加可减，但是减少还是使用coalesce，将这个理解为增加。(存在shuffle)
     */
    println("repartition")
    val repatitionResult: RDD[Int] = coalesceResult.repartition(5)
    println(repatitionResult.partitions.length)
    println()

    /**
     * sortBy：排序(存在shuffle)
     */
    println("sortBy")
    val rdd8: RDD[Int] = sc.parallelize(Array(4,1,3,8,6,7,5,2), 1)
    val sortResult1: RDD[Int] = rdd8.sortBy(x => x, false)
    val sortResult2: RDD[Int] = rdd8.sortBy(x => x, true)
    sortResult1.foreach(x => print(x + "\t"))
    println()
    sortResult2.foreach(x => print(x + "\t"))
    println()

  }
}
