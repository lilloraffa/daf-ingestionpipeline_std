package ingestion.pipelines.services

import org.apache.spark.sql.{DataFrame, SparkSession}

//Ontology & Voc Manager
//This is trying to mimic what will be offered directly through DAF and its managers.

object OntoMgr_old extends Serializable {
  //val vocCatMgr = new CatalogManager(voc_data_schema)
  val ontology = Map(
    "luoghi.stato.nome" -> Map("voc"->"voc_luoghi", "voc_col"->"stato", "hierc"->List("luoghi.stato")),
    "luoghi.regione.nome" -> Map("voc"->"voc_luoghi", "voc_col"->"regione", "hierc"->List("luoghi.stato", "luoghi.regione")),
    "luoghi.provincia.nome" -> Map("voc"->"voc_luoghi", "voc_col"->"prov", "hierc"->List("luoghi.stato", "luoghi.regione", "luoghi.provincia")),
    "luoghi.provincia.code" -> Map("voc"->"voc_luoghi", "voc_col"->"prov_cod", "hierc"->List("luoghi.stato", "luoghi.regione", "luoghi.provincia")),
    "luoghi.comune.nome" -> Map("voc"->"voc_luoghi", "voc_col"->"comune", "hierc"->List("luoghi.stato", "luoghi.regione", "luoghi.provincia", "luoghi.citta")),
    "luoghi.via.nome" -> Map("voc"->"voc_luoghi", "voc_col"->"via", "hierc"->List("luoghi.stato", "luoghi.regione", "luoghi.provincia", "luoghi.comune", "luoghi.via"))
  )

  def getDicDF(dicName: String, spark: SparkSession): Option[DataFrame] = DatasetLoader.dataset_loadDF(dicName, spark)
  def getOntoInfoField(tagName: String): Map[String, Any] = ontology(tagName)
  def getOnto(): Map[String, Map[String, Any]] = ontology
}
