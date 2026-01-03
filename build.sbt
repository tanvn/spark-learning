import scala.collection.Seq

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.18"

ThisBuild / evictionErrorLevel := Level.Error


lazy val root = (project in file("."))
  .settings(
    name := "spark-learning"
  )

// spark library dependencies
val sparkVersion = "4.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,
  // https://mvnrepository.com/artifact/org.apache.spark/spark-hive
  "org.apache.spark" %% "spark-hive" % sparkVersion
)