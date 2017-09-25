package it.gov.daf.ingestion.std

import org.apache.spark.sql.SparkSession

object IngestionProc {
  def main(args: Array[String]): Unit = {
    val appName = "IngestionStd"
    val master = "local[*]"

    val spark = SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()

    val ing = new IngestionStd()
    ing.init("ds_luoghi", spark)
    spark.stop()


  }
}
