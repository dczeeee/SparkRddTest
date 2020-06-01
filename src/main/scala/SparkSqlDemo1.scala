import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlDemo1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkSqlDemo1")
      .master("local[*]")
      .getOrCreate()

    val frame: DataFrame = spark.read.json(this.getClass.getClassLoader.getResource("student.json").getPath)

    frame.show()
    println(frame.schema)

    /**
     * 在使用sql查询之前需要注册临时视图
     *
     * createTempView()：注册视图，当前Session有效
     *
     * createOrReplaceTempView()：注册视图，当前Session有效，如果已经存在，那么替换
     *
     * createGlobalTempView()：注册全局视图，在所有Session中生效
     *
     * createOrReplaceGlobalTempView()：注册全局视图，在所有Session中生效，如果已经存在，那么替换
     */

    frame.createOrReplaceTempView("student")
    val result: DataFrame = spark.sql("select * from student where age >= 20")
    result.show()
  }
}
