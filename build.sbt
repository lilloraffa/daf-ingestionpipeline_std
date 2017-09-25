name := "daf-ingestpipeline_std"

version := "1.0"

scalaVersion := "2.11.1"

lazy val sparkVersion = "2.0.0"
lazy val spark = "org.apache.spark"

def dependencyToProvide(scope: String = "compile") = Seq(
  spark %% "spark-core" % sparkVersion % scope,
  spark %% "spark-sql" % sparkVersion % scope,
  spark %% "spark-streaming" % sparkVersion % scope
)

//libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"
libraryDependencies += "org.specs2" %% "specs2-core" % "3.9.5" % "test"

libraryDependencies ++= dependencyToProvide()