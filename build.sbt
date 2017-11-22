
import sbt.Keys.resolvers

name := "daf-ingestpipeline_std"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.1"

organization := "it.gov.daf"

lazy val sparkVersion = "2.0.0"
lazy val spark = "org.apache.spark"
val playVersion = "2.5.14"

def dependencyToProvide(scope: String = "compile") = Seq(
  spark %% "spark-core" % sparkVersion % scope,
  spark %% "spark-sql" % sparkVersion % scope,
  spark %% "spark-streaming" % sparkVersion % scope
)

//libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"
libraryDependencies ++= dependencyToProvide()

libraryDependencies ++= Seq(
  "com.typesafe.play" %% "play-ws" %  playVersion exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "it.gov.daf" %% "daf-catalog-manager-client" % "1.1-SNAPSHOT" % "compile",
  "org.specs2" %% "specs2-core" % "3.9.5" % "test"
)

resolvers ++= Seq(
  Resolver.mavenLocal,
  "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases",
  "jeffmay" at "https://dl.bintray.com/jeffmay/maƒven",
  "daf repo" at "http://nexus.default.svc.cluster.local:8081/repository/maven-public/"
)
