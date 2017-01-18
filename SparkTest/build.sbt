name := "SparkTest"

version := "1.0"

scalaVersion := "2.10.5"

val sparkVersion= "1.6.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
//libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"
libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.5.0"

resolvers+="Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"
libraryDependencies += "spark.jobserver" %% "job-server-api" % "0.6.2" % "provided"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}