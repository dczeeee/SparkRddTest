import java.text.DecimalFormat

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

object UdfDemo {
  def main(args: Array[String]): Unit = {

    //创建sparksession
    val spark = SparkSession.builder()
      .appName("UdfDemo")
      .master("local[*]")
      .getOrCreate()

    /**
     * 1.udf：一对一
     */

    //注册函数
    val toUpper: UserDefinedFunction = spark.udf.register("toUpper", (s: String) => s.toUpperCase)

    val df: DataFrame = spark.read.json(this.getClass.getClassLoader.getResource("student.json").getPath)

    df.createOrReplaceTempView("student")
    val result: DataFrame = spark.sql("select id,toUpper(name),age from student where age >= 20")
    result.show()

  }
}

class MyUdaf extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("input", LongType) :: Nil)

  override def bufferSchema: StructType = StructType(StructField("sum", DoubleType) :: StructField("count", LongType) :: Nil)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //sum
    buffer(0) = 0.0d
    //count
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //如果值不为空
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getDouble(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //如果值不为空
    if (!buffer2.isNullAt(0)) {
      buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }
  }

  override def evaluate(buffer: Row): Any = {
    new DecimalFormat(".00").format(buffer.getDouble(0) / buffer.getLong(1)).toDouble
  }

}
