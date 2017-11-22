package it.gov.daf.ingestion.std

import ingestion.pipelines.IngestionStd
import org.specs2._
import org.specs2.specification.ForEach
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.specs2.execute.AsResult


class IngestionStdSpec extends mutable.Specification with SparkCntx {

/*
  "Ingestion->Init works: " >> {spark: SparkSession =>
    val ing = new IngestionStd()
    //
    ing.init("ds_luoghi", spark) must beEqualTo("OK")
  }
*/

}

trait SparkCntx extends ForEach[SparkSession] {
  def foreach[R: AsResult](f: SparkSession =>R) ={
    val appName = "IngestionStd"
    val master = "local[*]"

    val spark = SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()

    try AsResult(f(spark))
    finally spark.stop()
  }
}
