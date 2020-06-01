import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Rdd2Frame2Set {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Rdd2Frame2Set")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val rdd: RDD[String] = spark.sparkContext.textFile(this.getClass.getClassLoader.getResource("student.csv").getPath)

    val studentRdd: RDD[Student] = rdd.map(x => {
      val line: Array[String] = x.split(",")
      Student(line(0).toLong, line(1), line(2).toLong)
    })

    /**
     * 1.rdd2df
     */
    val rdd2df: DataFrame = studentRdd.toDF()
    rdd2df.show()

    /**
     * 2.rdd2ds
     */
    val rdd2ds: Dataset[Student] = studentRdd.toDS()
    rdd2ds.show()

    /**
     * 3.df2rdd
     */
    val df2rdd: RDD[Row] = rdd2df.rdd
    println(df2rdd.collect().toList)

    /**
     * 4.ds2rdd
     */
    val ds2rdd: RDD[Student] = rdd2ds.rdd
    println(ds2rdd.collect().toList)

    /**
     * 5.ds2df
     */
    val ds2df: DataFrame = rdd2ds.toDF()
    ds2df.show()

    /**
     * 6.df2ds
     */
    val df2ds: Dataset[Student] = rdd2df.as[Student]
    df2ds.show()

  }
}

case class Student(id: Long, name: String, age: Long)
