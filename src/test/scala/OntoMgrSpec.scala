package it.gov.daf.ingestion.std

import ingestion.pipelines.services.OntoMgr_old
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.specs2._

class OntoMgrSpec extends mutable.Specification with SparkCntx{

  "OntoMgr.getDicDF works: " >> {spark: SparkSession =>
    OntoMgr_old.getDicDF("ds_luoghi", spark) must beSome[DataFrame]
  }

}
