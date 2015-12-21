package kinesis

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kinesis.KinesisUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by ytaras on 12/18/15.
  */
object SampleSparkParserJob extends App {
  Class.forName("org.postgresql.Driver")
//"jdbc:postgresql://192.168.99.100:32771/mock_ps?user=postgres"
  val Array(appName, streamName, endpointUrl, dbUrl, output) = args
  val shardsNum = Util.kinesisShards(endpointUrl, streamName)
  val numStreams = shardsNum
  val regionName = Util.regionName(endpointUrl)


  val batchInterval = Seconds(15)
  val kinesisCheckpointInterval = Seconds(15)

  // Setup the SparkConfig and StreamingContext
  val sparkConfig = new SparkConf().setAppName("KinesisWordCountASL").setMaster("local[*]")
  val sc = new SparkContext(sparkConfig)
  val sqlContext = new SQLContext(sc)
  val ssc = new StreamingContext(sc, batchInterval)

  // Create the Kinesis DStreams
  val kinesisStreams = (0 until numStreams).map { i =>
    KinesisUtils.createStream(ssc, appName, streamName, endpointUrl, regionName,
      InitialPositionInStream.TRIM_HORIZON, kinesisCheckpointInterval, StorageLevel.MEMORY_AND_DISK_2)
  }

  val metadata = sqlContext.read.format("jdbc")
    .option("url", dbUrl).option("dbTable", "word")
    .option("numPartitions", "2")
    .load.cache

  println(metadata.count())

//  val stream = ssc.socketTextStream("localhost", 9999).map { _.getBytes }
  val stream = ssc.union(kinesisStreams)
  parseAndEnrich(stream, metadata)
    .repartition(1)
    .saveAsTextFiles(output)
  ssc.start()
  ssc.awaitTermination()


  def parseAndEnrich(stream: DStream[Array[Byte]], metadata: DataFrame): DStream[String] = {
    import sqlContext.implicits._
    val mdDF = metadata.as('meta)
    stream
      .map { new String(_) }
      .flatMap { _.split("\\s+") }
      .transform[String] { wordsRdd: RDD[String] =>

      wordsRdd.toDF("word").as('source)
        .join(mdDF, $"source.word" === $"meta.word", "left_outer").toJSON
    }
  }
}
