name := "spark_kinesis"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.amazonaws" % "amazon-kinesis-client" % "1.6.1",
  "org.apache.hadoop" % "hadoop-aws" % "2.7.1" intransitive(),
  "postgresql" % "postgresql" % "9.1-901-1.jdbc4",
//  "com.amazonaws" % "aws-java-sdk" % "1.9.16",
  "org.apache.spark" %% "spark-streaming-kinesis-asl" % "1.5.2" intransitive(),
  "org.apache.spark" %% "spark-streaming" % "1.5.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.5.2" % "provided",
  "com.databricks" %% "spark-redshift" % "0.5.2"
)

mainClass in assembly := Some("kinesis.KinesisWordCountASL")

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))