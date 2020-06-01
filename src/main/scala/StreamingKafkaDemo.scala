import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingKafkaDemo {
  def main(args: Array[String]): Unit = {

    val brokers = "cdh01.cm:9092,cdh02.cm:9092,cdh03.cm:9092"
    val topic = "bigdata"
    val cgroup = "test"
    val params: Map[String, Object] = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> cgroup,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val conf = new SparkConf()
      .setAppName("StreamingKafkaDemo")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setMaster("local[*]")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(10))

    // Streaming对接kafka
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List(topic), params)
    )

    kafkaDStream.print

    //启动StreamingContext并等待终止
    ssc.start()
    ssc.awaitTermination()

  }
}
