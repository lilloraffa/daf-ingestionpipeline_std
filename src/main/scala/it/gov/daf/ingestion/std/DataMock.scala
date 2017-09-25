package it.gov.daf.ingestion.std


object DataMock {
  //Metadata Info per dataset
  val catalog_dataschema = Map(
    "ds_luoghi" -> Map(

      "namespace" -> "it.daf.ds.ordinary",
      "type" -> "record",
      "name" -> "test",
      "fields" -> Seq(
        Map(
          "name"->"id",
          "type"->"int"
        ),
        Map(
          "name"->"comune",
          "type"->"String",
          "metadata" -> Map(
            "semantics"->"luoghi.comune.nome",
            "tag" -> "comune, residenza",
            "fields_connection" -> "luogo_residenza"
          )
        ),
        Map(
          "name"->"provincia",
          "type"->"String",
          "metadata" -> Map(
            "semantics"->"luoghi.provincia.nome",
            "tag" -> "provincia, residenza",
            "fields_connection" -> "luogo_residenza"
          )
        ),
        Map(
          "name"->"provincia_code",
          "type"->"String",
          "metadata" -> Map(
            "semantics"->"luoghi.provincia.code",
            "tag" -> "provincia, residenza",
            "fields_connection" -> "luogo_residenza"
          )
        ),
        Map(
          "name"->"regione",
          "type"->"String",
          "metadata" -> Map(
            "semantics"->"luoghi.regione.nome",
            "tag" -> "regione, residenza",
            "fields_connection" -> "luogo_residenza"
          )
        ),
        Map(
          "name"->"pop",
          "type"->"int"
        )
      )
    ),
    "voc_luoghi" -> Map(

      "namespace" -> "it.daf.ds.voc",
      "type" -> "record",
      "name" -> "voc_luoghi",
      "fields" -> Seq(
        Map(
          "name"->"id",
          "type"->"int",
          "metadata" -> Map(
            "semantics"->"luoghi.id.val"
          )
        ),
        Map(
          "name"->"comune",
          "type"->"String",
          "metadata" -> Map(
            "semantics"->"luoghi.comune.nome"
          )
        ),
        Map(
          "name"->"prov",
          "type"->"String",
          "metadata" -> Map(
            "semantics"->"luoghi.provincia.nome"
          )
        ),
        Map(
          "name"->"prov_code",
          "type"->"String",
          "metadata" -> Map(
            "semantics"->"luoghi.provincia.code"
          )
        ),
        Map(
          "name"->"regione",
          "type"->"String",
          "metadata" -> Map(
            "semantics"->"luoghi.regione.nome"
          )
        ),
        Map(
          "name"->"stato",
          "type"->"String",
          "metadata" -> Map(
            "semantics"->"luoghi.stato.nome"
          )
        )
      )
    )
  )

  val catalog_dcat = Map(
    "ds_luoghi" -> Map("" ->""),
    "voc_luoghi" -> Map("" ->"")
  )

  val catalog_operational = Map(
    "ds_luoghi" -> Map("" ->""),
    "voc_luoghi" -> Map("" ->"")
  )

  // Voc Controllato
  val dicLuoghi = List(
    List("1", "Santeramo", "Verona", "VE", "Veneto", "Italia"),
    List("2", "Bari","Bari", "BA", "Puglia", "Italia"),
    List("3", "Bitritto","Bari", "BA", "Puglia", "Italia"),
    List("4", "Santeramo in Colle","Bari", "BA", "Puglia", "Italia")
  )

  // Dataset
  val dataLuoghi = List(
    List("1", "Santeramo", "Verona", "VE", "Veneto", "1000"),
    List("2", "Bari","Bari", "BA", "Puglia", "2000"),
    List("3", "Bitritto","Bari", "BA", "Puglia", "3000"),
    List("4", "Santeramo in Colle","Bari", "BA", "Puglia", "1000"),
    List("5", "Santrmano in Colli","Bari", "BA", "Puglia", "3000")
  )

  val data = Map(
    "ds_luoghi" -> dataLuoghi,
    "voc_luoghi" -> dicLuoghi
  )

  def getData(ds_name: String): List[List[String]] = data.getOrElse(ds_name, List())
  def getCatalogMetadata(ds_uri: String, metadata: String): Map[String, Any] = {
    metadata match {
      case "dcatap" => catalog_dcat.getOrElse(ds_uri, Map())
      case "dataschema" => catalog_dataschema.getOrElse(ds_uri, Map())
      case "operational" => catalog_operational.getOrElse(ds_uri, Map())
    }
  }
  def getCatalogMetadata(ds_uri: String): Map[String, Any] = {
    Map(
      "dcatap" -> catalog_dcat.getOrElse(ds_uri, Map()),
      "dataschema" -> catalog_dataschema.getOrElse(ds_uri, Map()),
      "operational" -> catalog_operational.getOrElse(ds_uri, Map())
    )
  }

}
