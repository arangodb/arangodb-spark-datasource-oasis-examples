name := "oasis_scala_spark_demo_2.11"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"
libraryDependencies += "com.arangodb" %% "arangodb-spark-datasource-2.4" % "0.1.0-SNAPSHOT"

resolvers ++= Seq(
  Resolver.sonatypeRepo("public")
)