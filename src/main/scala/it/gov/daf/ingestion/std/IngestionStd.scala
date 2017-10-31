package it.gov.daf.ingestion.std

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import it.gov.daf.ingestion.std.lib.TextMatcher
//import it.gov.daf.ingestion.std.DataMock

class IngestionStd {


  //Given a ds_uri, it will start the ingestion pipeline and manage all spark work
  def init(ds_uri: String, spark: SparkSession): String = {

    val dataCatMgr = new CatalogManager(ds_uri)
    val df_data = DatasetLoader.dataset_loadDF(ds_uri, spark)

    //[col_name, (semantic_tag, hierarchy, voc name, voc col)]
    val ontoTagHiercRaw: Seq[(String, (String, List[String], String, String))] = dataCatMgr.ontoTag.map{x =>
      x._1 -> (x._2, OntoMgr.getOntoInfoField(x._2)("hierc").asInstanceOf[List[String]], OntoMgr.getOntoInfoField(x._2)("voc").asInstanceOf[String], OntoMgr.getOntoInfoField(x._2)("voc_col").asInstanceOf[String])
    }.toSeq


    //
    val dictInvolved: Map[String, Int] = ontoTagHiercRaw.map(x => x._2._3).groupBy(x=>x).map(x=>(x._1 -> x._2.size))

    val ontoTagHierc = ontoTagHiercRaw.sortBy(x=> (dictInvolved(x._2._3), x._2._3, x._2._2.length)).to[List]

    val df_final = df_data match {
      case Some(df) => Some(stdFunc(df, ontoTagHierc, dataCatMgr, spark: SparkSession))
      case _ => None
    }

    df_final match {
      case Some(s) => {
        //Save Dataset
        s.show()
        "OK"
      }
      case _ => "ERROR"
    }
  }

  def stdFunc(df_data: DataFrame, ontoTagHierc: List[(String, (String, List[String], String, String))], dataCatMgr: CatalogManager, spark: SparkSession): DataFrame = {
    // Everytime the rec internal function is called, it produces a full standardization of a column
    // and return a new dataframe that join the initial with the new std column (+ a col with stats of the standardization)
    import spark.implicits._

    def stdFuncAcc(df_data: DataFrame, df_vocIn: Option[DataFrame], ontoTagHierc: List[(String, (String, List[String], String, String))], prevVoc: String): DataFrame = {
      ontoTagHierc match {
        case Nil => df_data
        case x :: tail => {
          //Get Voc Name
          val vocName = x._2._3
          val vocCatMgr = new CatalogManager(vocName)

          //Load a new Voc or reuse the previous one
          val df_vocOpt: Option[DataFrame] = df_vocIn match {
            case Some(s) if vocName.equals(prevVoc) => Some(s)
            case _ => OntoMgr.getDicDF(vocName, spark)
          }

          //Match if a voc has been found, else pass the same dataframe (aka, don't do anything)
          df_vocOpt match {
            case Some(df_voc) => {
              //Nome della colonna da standardizzare
              //Step 1, inizio dalla prima in ordine di gerarchia
              //val x = ontoTagHierc(0)
              val colName = x._1
              //Nome del tag semantico della colonna da standardizzare
              val tagCol = x._2._1
              //Prendo le entita' correlate da utilizzare come filtro del controlled vocabolary
              //Questo serve per restringere il più possibile il campo delle possibili traduzioni da vocabolario controllato
              val entityLinked = x._2._2.filter{x => x != tagCol}
              //Prendi le colonne gia' standardizzate correlate alle linked entities
              //Le colonne già standardizzate vengono appese al dataset con la naming convention "__std_[nome colonna]"
              val colLinkedNames: List[String] =
                df_data.columns
                  .filter(x=>x.startsWith("__std_"))
                  .map(x => x.replace("__std_", "")).toList
                  // .filter{x =>
                  .filter{x =>
                    val colTag = dataCatMgr.ontoTag.get(x)
                    colTag match {
                      case Some(s) => {
                        val endIndex = s.lastIndexOf(".")
                        val entity = s.substring(0,endIndex)
                        entityLinked.contains(entity)
                        //& s != colTag
                      }
                      case _ => false
                    }

                  //colTag.equals("luoghi.regione.nome")
                  }


              //Get the columns that need to be used in the df_data select (the col to be std and the correlated ones that have been already standardized)
              val colSelect_data: List[String] = colName :: (colLinkedNames.toList)
              val colSelect_voc: List[String] = colSelect_data.map{x=>
                val colTag = dataCatMgr.ontoTag.get(x)
                colTag match {
                  case Some(s) => vocCatMgr.ontoTag.find(x=> x._2.equals(s)).map(x=>x._1)
                  case _ => None
                }
              }.filter{x=>x.isDefined}.map{x=>x.get}

              //get the table of the vocabulary to be used for standardization, with the columns needed for the matching and filter to restrict
              val voc = df_voc.select(colSelect_voc.head, colSelect_voc.tail: _*).distinct.rdd.collect()

              //produce the dataframe with the result of stdInference function (standardized col + score)
              val df_conv = df_data.select(colSelect_data.head, colSelect_data.tail: _*).distinct.rdd.map{x=>
                //val voc_custom = voc.
                val xSeq = x.toSeq.map(x=> x.asInstanceOf[String])
                if (xSeq.length >1) {
                  val dataLinkedCol = xSeq.tail.map(x=>x.asInstanceOf[String].toLowerCase)
                  val voc_cust = voc.filter(x=> x.toSeq.map(x=>x.asInstanceOf[String].toLowerCase).tail.equals(dataLinkedCol)).map(x=>x(0).asInstanceOf[String])
                  TextMatcher.doit("levenshtein")(xSeq.head, voc_cust)
                } else {
                  val voc_cust = voc.map(x=>x(0).asInstanceOf[String])
                  TextMatcher.doit("levenshtein")(xSeq.head, voc_cust)
                }
              }

              val df_data_joined = df_data.join(df_conv.toDF(colName, "__std_"+colName, "__stdscore_"+colName), Seq(colName), "left")
              stdFuncAcc(df_data_joined, Some(df_voc), tail, vocName)
            }
            case _ => stdFuncAcc(df_data, None, tail, vocName)
          }
        }
      }

    }

    stdFuncAcc(df_data, None, ontoTagHierc, "-1")

  }


  //New Development to generalize std class
  def stdFunc2(df_data: DataFrame, ontoTagHierc: List[(String, (String, List[String], String, String))], dataCatMgr: CatalogManager, spark: SparkSession): DataFrame = {
    // Everytime the rec internal function is called, it produces a full standardization of a column
    // and return a new dataframe that join the initial with the new std column (+ a col with stats of the standardization)
    import spark.implicits._

    def stdFuncAcc(df_data: DataFrame, df_vocIn: Option[DataFrame], ontoTagHierc: List[(String, (String, List[String], String, String))], prevVoc: String): DataFrame = {
      ontoTagHierc match {
        case Nil => df_data
        case x :: tail => {
          //Get Voc Name
          val vocName = x._2._3
          val vocCatMgr = new CatalogManager(vocName)

          //Load a new Voc or reuse the previous one
          val df_vocOpt: Option[DataFrame] = df_vocIn match {
            case Some(s) if vocName.equals(prevVoc) => Some(s)
            case _ => OntoMgr.getDicDF(vocName, spark)
          }

          //Match if a voc has been found, else pass the same dataframe (aka, don't do anything)
          df_vocOpt match {
            case Some(df_voc) => {
              //Nome della colonna da standardizzare
              //Step 1, inizio dalla prima in ordine di gerarchia
              //val x = ontoTagHierc(0)
              val colName = x._1
              //Nome del tag semantico della colonna da standardizzare
              val tagCol = x._2._1
              //Prendo le entita' correlate da utilizzare come filtro del controlled vocabolary
              //Questo serve per restringere il più possibile il campo delle possibili traduzioni da vocabolario controllato
              val entityLinked = x._2._2.filter{x => x != tagCol}
              //Prendi le colonne gia' standardizzate correlate alle linked entities
              //Le colonne già standardizzate vengono appese al dataset con la naming convention "__std_[nome colonna]"
              val colLinkedNames: List[String] =
              df_data.columns
                .filter(x=>x.startsWith("__std_"))
                .map(x => x.replace("__std_", "")).toList
                // .filter{x =>
                .filter{x =>
                val colTag = dataCatMgr.ontoTag.get(x)
                colTag match {
                  case Some(s) => {
                    val endIndex = s.lastIndexOf(".")
                    val entity = s.substring(0,endIndex)
                    entityLinked.contains(entity)
                    //& s != colTag
                  }
                  case _ => false
                }

                //colTag.equals("luoghi.regione.nome")
              }


              //Get the columns that need to be used in the df_data select (the col to be std and the correlated ones that have been already standardized)
              val colSelect_data: List[String] = colName :: (colLinkedNames.toList)
              val colSelect_voc: List[String] = colSelect_data.map{x=>
                val colTag = dataCatMgr.ontoTag.get(x)
                colTag match {
                  case Some(s) => vocCatMgr.ontoTag.find(x=> x._2.equals(s)).map(x=>x._1)
                  case _ => None
                }
              }.filter{x=>x.isDefined}.map{x=>x.get}

              //get the table of the vocabulary to be used for standardization, with the columns needed for the matching and filter to restrict
              val voc = df_voc.select(colSelect_voc.head, colSelect_voc.tail: _*).distinct.rdd.collect()

              //produce the dataframe with the result of stdInference function (standardized col + score)
              val df_conv = df_data.select(colSelect_data.head, colSelect_data.tail: _*).distinct.rdd.map{x=>
                //val voc_custom = voc.
                val xSeq = x.toSeq.map(x=> x.asInstanceOf[String])
                if (xSeq.length >1) {
                  val dataLinkedCol = xSeq.tail.map(x=>x.asInstanceOf[String].toLowerCase)
                  val voc_cust = voc.filter(x=> x.toSeq.map(x=>x.asInstanceOf[String].toLowerCase).tail.equals(dataLinkedCol)).map(x=>x(0).asInstanceOf[String])
                  TextMatcher.doit("levenshtein")(xSeq.head, voc_cust)
                } else {
                  val voc_cust = voc.map(x=>x(0).asInstanceOf[String])
                  TextMatcher.doit("levenshtein")(xSeq.head, voc_cust)
                }
              }

              val df_data_joined = df_data.join(df_conv.toDF(colName, "__std_"+colName, "__stdscore_"+colName), Seq(colName), "left")
              stdFuncAcc(df_data_joined, Some(df_voc), tail, vocName)
            }
            case _ => stdFuncAcc(df_data, None, tail, vocName)
          }
        }
      }

    }

    stdFuncAcc(df_data, None, ontoTagHierc, "-1")

  }


}
