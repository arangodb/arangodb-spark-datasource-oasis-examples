name := "oasis_scala_spark_demo_2.12"

version := "0.1"

scalaVersion := "2.12.15"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"
libraryDependencies += "com.arangodb" %% "arangodb-spark-datasource-3.1" % "0.1.0-SNAPSHOT"

resolvers ++= Seq(
  Resolver.sonatypeRepo("public")
)