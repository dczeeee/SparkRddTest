import org.apache.spark.sql.{Dataset, SparkSession}

object DataSetDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataSetDemo")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val ds: Dataset[Student] = Seq(Student(1, "zhangsan", 15), Student(2, "lisi", 16)).toDS()

    ds.foreach(s => {
      println(s.name + " : " + s.age)
    })
  }
}

case class Student(id: Long, name: String, age: Long)