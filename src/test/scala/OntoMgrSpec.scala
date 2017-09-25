package it.gov.daf.ingestion.std

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.specs2._

class OntoMgrSpec extends mutable.Specification with SparkCntx{

  "OntoMgr.getDicDF works: " >> {spark: SparkSession =>
    OntoMgr.getDicDF("ds_luoghi", spark) must beSome[DataFrame]
  }

}
