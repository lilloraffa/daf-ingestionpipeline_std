package it.gov.daf.ingestion.std

import org.apache.spark.sql.{DataFrame, SparkSession}

object DatasetLoader extends Serializable{
  //Function to load dataset from DAF. When in production, this needs to connect to the dataset manager directly
  def dataset_loadDF(ds_uri: String, spark: SparkSession): Option[DataFrame] = {
    import spark.implicits._

    ds_uri match {
      case s if s.equals("ds_luoghi") => {
        val colName = (new CatalogManager(s)).colName
        Some(DataMock.getData(s).map{case List(a,b,c,d,e,f) => (a,b,c,d,e,f)}.toDF(colName: _*))
      }
      case s if s.equals("voc_luoghi") => {
        val colName = (new CatalogManager(s)).colName
        Some(DataMock.getData(s).map{case List(a,b,c,d,e,f) => (a,b,c,d,e,f)}.toDF(colName: _*))
      }
      case _ => None
    }
  }
}
