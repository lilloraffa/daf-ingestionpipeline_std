package it.gov.daf.ingestion.std

class CatalogManager(ds_uri: String) extends Serializable {

  val data_schema: Map[String, Any] = DataMock.getCatalogMetadata(ds_uri, "dataschema")

  val colName: Seq[String] =
    data_schema("fields").asInstanceOf[Seq[Map[String, Object]]]
      .map(x => x("name").asInstanceOf[String])

  val ontoTag: Map[String, String] =
    data_schema("fields").asInstanceOf[Seq[Map[String, Object]]]
      .map{x =>
        val metadata = x.asInstanceOf[Map[String, Object]].get("metadata")
        val semantics = metadata match{
          case Some(s) => s.asInstanceOf[Map[String,String]].get("semantics")
          case None => None
        }
        x("name").asInstanceOf[String] -> semantics
      }
    .filter(x=>x._2.isDefined).map(x=> x._1->x._2.get).toMap
}
