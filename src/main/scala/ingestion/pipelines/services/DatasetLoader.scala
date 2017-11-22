package ingestion.pipelines.services

import org.apache.spark.sql.{DataFrame, SparkSession}

object DatasetLoader extends Serializable{
  //Function to load dataset from DAF. When in production, this needs to connect to the dataset manager directly
  def dataset_loadDF(ds_uri: String, spark: SparkSession): Option[DataFrame] = {

    /*
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
    */
//TODO This needs to be changed with DatasetMgr / StorageMgr
    ds_uri match {
      case s if s.equals("ds_luoghi") => {
        Some(spark.read.parquet("data/ds_luoghi"))
      }
      case s if s.equals("voc_luoghi") => {
        Some(spark.read.parquet("data/voc_luoghi"))
      }
      case s if s.equals("daf://dataset/ord/raffaele/raffaele/theme/ds_poi") => {
        Some(spark.read.parquet("data/ds_poi"))
      }
      case s if s.equals("http://dati.gov.it/onto/controlledvocabulary/POICategoryClassification") => {
        Some(spark.read.parquet("data/voc_poi"))
      }
      case _ => None
    }


  }


}
