package kinesis

import java.io.File

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by ytaras on 12/18/15.
  */
object SampleSparkAggregationJob extends App {
  val Array(input, db, out) = args

  val sparkConfig = new SparkConf().setAppName("Aggregation").setMaster("local[*]")
  val sc = new SparkContext(sparkConfig)
  Util.loadDrivers(sc)
  val sqlContext = new SQLContext(sc)

  val staging = sqlContext.read.json(sc.textFile(input))
  val aggregated = staging.groupBy("word").count()
  val metadata = sqlContext.read.format("jdbc")
    .option("url", db).option("dbTable", "word")
    .option("numPartitions", "2")
    .option("driver", "org.postgresql.Driver")
    .load.cache


  import sqlContext.implicits._

  val enriched = aggregated.as('agg)
    .join(metadata.withColumnRenamed("word", "meta_word"), $"agg.word" === $"meta_word", "left_outer")
    .select("word", "count", "metadata")
  enriched.repartition(1).write.json(out)

}
