import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AnyByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AnyByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.parallelize(Array("HADOOP", "SPARK", "HIVE", "FLINK", "HIVE", "FLINK"), 2)
    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))
    rdd2.mapPartitionsWithIndex((index ,it) => {
      it.map((index, _))
    }).foreach(println)
    println()

    /**
     * reduceByKey(V , V)=>V	根据key进行聚合，在shuffle之前会有combine(预聚合)操作
     */
    println("reduceByKey")
    val result1: RDD[(String, Int)] = rdd2.reduceByKey(_+_)
    result1.foreach(x => print(x + "\t"))
    println()

    /**
     * groupByKey：根据key进行分组，直接shuffle
     */
    println("groupByKey")
    val result2: RDD[(String, Iterable[Int])] = rdd2.groupByKey()
    result2.foreach(x => print(x + "\t"))
    println()
    result2.map(x => (x._1, x._2.size)).foreach(x => print(x + "\t"))
    println()

    /**
     * aggrateByKey(zero : U)(( U , V )=>U , (U , U)=>U)
     * 个人理解，（初始值）（分区内函数，分区间函数）
     * 基于Key分组然后去聚合的操作，耗费资源太多，这时可以使用reduceByKey或aggrateByKey算子去提高性能
     * aggrateByKey分区内聚合，后在进行shuffle聚合。
     */
    println("aggrateByKey")
    val result3: RDD[(String, Int)] = rdd2.aggregateByKey(0)(_+_, _-_)
    result3.foreach(x => print(x + "\t"))
    println()

    /**
     * foldByKey(zero : V)((V , V)=>V)	折叠计算，没有aggrateByKey灵活，如果分区内和分区外聚合计算不一样，则不行
     * 个人理解，（初始值）（分区内和分区间共用同个函数）
     */
    println("foldByKey")
    val result4: RDD[(String, Int)] = rdd2.foldByKey(0)(_-_)
    result4.foreach(x => print(x + "\t"))
    println()

    /**
     * combineByKey(V=>U,(U , V)=>U , (U , U)=>U)	根据Key组合计算
     * 1. value作为参数
     * 2. 1中的参数与分区内参数计算
     * 3. 分区间2计算
     */
    println("combineByKey")
    val result5: RDD[(String, Int)] = rdd2.combineByKey(
      v => v,
      (c: Int, v: Int) => c + v,
      (c1: Int, c2: Int) => c1 + c2
    )
    result5.foreach(x => print(x + "\t"))
    println()
    val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))
    val d1 = sc.parallelize(initialScores)
    type MVType = (Int, Double) //定义一个元组类型(科目计数器,分数)
    d1.combineByKey(
      score => (1, score), // (1, 88.0)
      (c1: MVType, newScore) => (c1._1 + 1, c1._2 + newScore), // ((1, 88.0), 91.0) => (2, 88.0+91.0)
      (c1: MVType, c2: MVType) => (c1._1 + c2._1, c1._2 + c2._2) // ((2, 88.0+91.0),(1,95.0)) => (3, 88.0+91.0+95.0)
    ).map { case (name, (num, socre)) => (name, socre / num) }.collect.foreach(println) // (Fred, (3, 88.0+91.0+95.0)) => (Fred, (88+91+95)/3)
    println()

    /**
     * sortByKey：按照key排序
     */
    println("sortByKey")
    val result6 = rdd2.sortByKey(false)
    result6.foreach(x => print(x + "\t"))
    println()
    val result7 = rdd2.sortByKey(true)
    result7.foreach(x => print(x + "\t"))
    println()

    /**
     * mapValues:只对value操作的map转换操作
     */
    println("mapValues")
    val result8 = rdd2.mapValues(_ + 100)
    result8.foreach(x => print(x + "\t"))
    println()

    /**
     * join
     */
    println("join")
    val rdd3: RDD[(String, Int)] = sc.parallelize(Array(("a",10),("b",10),("a",20),("d",10)))
    val rdd4: RDD[(String, Int)] = sc.parallelize(Array(("a",30),("b",20),("c",10)))
    //内连接 (a,(10,30))(b,(10,20))(a,(20,30))
    rdd3.join(rdd4).foreach(x => print(x + "\t"))
    println()
    //左链接(b,(10,Some(20)))(d,(10,None))(a,(10,Some(30)))(a,(20,Some(30)))
    rdd3.leftOuterJoin(rdd4).foreach(x => print(x + "\t"))
    println()
    //右链接(c,(None,10))(a,(Some(10),30))(b,(Some(10),20))(a,(Some(20),30))
    rdd3.rightOuterJoin(rdd4).foreach(x => print(x + "\t"))
    println()
    //全链接(b,(Some(10),Some(20)))(c,(None,Some(10)))(d,(Some(10),None))(a,(Some(10),Some(30)))(a,(Some(20),Some(30)))
    rdd3.fullOuterJoin(rdd4).foreach(x => print(x + "\t"))
    println()

    /**
     * cogroup：根据Key聚合RDD
     */
    println("cogroup")
    rdd3.cogroup(rdd4).foreach(println)

  }
}
