name := "spark_kinesis"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.amazonaws" % "amazon-kinesis-client" % "1.3.0",
//  "com.amazonaws" % "aws-java-sdk" % "1.9.16",
  "org.apache.spark" %% "spark-streaming-kinesis-asl" % "1.5.2" intransitive(),
  "org.apache.spark" %% "spark-streaming" % "1.5.2" % "provided"
)

mainClass in assembly := Some("kinesis.KinesisWordCountASL")