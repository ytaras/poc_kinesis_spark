package kinesis

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.AmazonKinesisClient
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext

/**
  * Created by ytaras on 12/18/15.
  */
object Util {
  def regionName(endpointUrl: String): String = {
    RegionUtils.getRegionByEndpoint(endpointUrl).getName
  }


  def kinesisShards(endpointUrl: String, streamName: String): Int = {
    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials
    require(credentials != null,
      "No AWS credentials found. Please specify credentials using one of the methods specified " +
        "in http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html")
    val kinesisClient = new AmazonKinesisClient(credentials)
    kinesisClient.setEndpoint(endpointUrl)
    kinesisClient.describeStream(streamName).getStreamDescription.getShards.size
  }

  def loadDrivers(sc: SparkContext): Unit = {
    Class.forName("org.postgresql.Driver")
    val hadoopConf: Configuration = sc.hadoopConfiguration
    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
  }
}

case class Word(word: String, description: Option[String])
