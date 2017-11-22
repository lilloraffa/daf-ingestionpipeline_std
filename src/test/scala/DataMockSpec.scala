package it.gov.daf.ingestion.std

import ingestion.pipelines.services.DataMock
import org.specs2._
import org.specs2.specification.ForEach
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.specs2.execute.AsResult

class DataMockSpec extends mutable.Specification{

  "getData should: " should {
    "return a list of list with param: 'ds_luoghi': " in {
      //DataMock.getData("ds_luoghi").getClass().toString must beEqualTo("class scala.collection.immutable.$colon$colon")
      DataMock.getData("ds_luoghi") must beAnInstanceOf[List[List[String]]]
    }

    "return a list of list with param: 'voc_luoghi': " in {
      DataMock.getData("voc_luoghi") must beAnInstanceOf[List[List[String]]]
    }
  }

  "getCatalogMetadata should: " should {
    "without param -> return a Map[String, Any]: " in {
      DataMock.getCatalogMetadata("ds_luoghi", "dataschema") must beAnInstanceOf[Map[String, Any]]
    }

    "with param 'dataschema' -> return a Map[String, Any]: " in {
      DataMock.getCatalogMetadata("dataschema") must beAnInstanceOf[Map[String, Any]]
    }
  }


}
