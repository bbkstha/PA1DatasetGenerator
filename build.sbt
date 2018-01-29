name := "PA1DatasetGenerator"

version := "0.1"

scalaVersion := "2.11.10"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // https://mvnrepository.com/artifact/edu.umd/cloud9
  "edu.umd" % "cloud9" % "1.5.0"
)
