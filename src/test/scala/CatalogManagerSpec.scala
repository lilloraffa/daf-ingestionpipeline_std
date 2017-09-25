package it.gov.daf.ingestion.std

import org.specs2._

class CatalogManagerSpec extends mutable.Specification{

  "CatalogManager.data_schema should: " should {
    "with ds_uri: 'ds_luoghi', return a Map[String, Any] " in {
      val catMgr = new CatalogManager("ds_luoghi")
      catMgr.data_schema must beAnInstanceOf[Map[String, Any]]
    }
  }

  "CatalogManager.colName should: " should {
    "with ds_uri: 'ds_luoghi', return a List[String] " in {
      val catMgr = new CatalogManager("ds_luoghi")
      catMgr.colName must beAnInstanceOf[List[String]]
    }
  }

  "CatalogManager.ontoTag should: " should {
    "with ds_uri: 'ds_luoghi', return a List[String] " in {
      val catMgr = new CatalogManager("ds_luoghi")
      catMgr.ontoTag must beAnInstanceOf[Map[String, Any]]
    }
  }



}
