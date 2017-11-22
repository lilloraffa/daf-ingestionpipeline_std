package ingestion.pipelines

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import lib.TextMatcher
import play.api.libs.ws.WSClient
import ingestion.pipelines.data.IngestionClass.IngPipeReport
import ingestion.pipelines.services.{CatalogMgr, DatasetLoader, OntoMgr}

import scala.concurrent.ExecutionContext


class IngestionStd(implicit ws: WSClient, ex: ExecutionContext, spark: SparkSession, auth: String) extends IngestionPipeline {
  val ingPipelineName = "IngestionStd"

  val startDatetime: String = (System.currentTimeMillis / 1000).toString
  val pipelineName: String = ConfigFactory.load().getString("IngPipeline.Std")

  val addedColVal: String = "__std_"
  val addedColStat: String = "__stdscore_"

  //Given a ds_uri, it will start the ingestion pipeline and manage all spark work
  def initBatch(ds_uri: String, dataCatMgr: CatalogMgr, df_data: DataFrame): (Option[DataFrame], IngPipeReport) = {

    val ontoMgr = OntoMgr(dataCatMgr.ontoTag.map(x=>x._2).toList)

    //[col_name, (semantic_tag, hierarchy, voc name, voc col)]
    val ontoTagHiercRaw: Seq[(String, (String, List[String], String, String))] = dataCatMgr.ontoTag.map{x =>
      x._1 -> (x._2, ontoMgr.getOntoInfoField(x._2).hierc, ontoMgr.getOntoInfoField(x._2).vocName, ontoMgr.getOntoInfoField(x._2).vocCol)
    }.toSeq

    val dictInvolved: Map[String, Int] = ontoTagHiercRaw.map(x => x._2._3).groupBy(x=>x).map(x=>(x._1 -> x._2.size))

    val ontoTagHierc = ontoTagHiercRaw.sortBy(x=> (dictInvolved(x._2._3), x._2._3, x._2._2.length)).to[List]
    println(ontoTagHierc)

    try{
      val df_final = stdFunc(df_data, ontoTagHierc, dataCatMgr, spark: SparkSession)
      val endDatetime: String = (System.currentTimeMillis / 1000).toString
      (Some(df_final), IngPipeReport(ds_uri, startDatetime, endDatetime, ingPipelineName, "ok"))
    } catch {
      case _ =>
        val endDatetime: String = (System.currentTimeMillis / 1000).toString
        (None, IngPipeReport(ds_uri, startDatetime, endDatetime, ingPipelineName, "error"))

    }

  }

  def stdFunc(df_data: DataFrame, ontoTagHierc: List[(String, (String, List[String], String, String))], dataCatMgr: CatalogMgr, spark: SparkSession): DataFrame = {
    // Everytime the rec internal function is called, it produces a full standardization of a column
    // and return a new dataframe that join the initial with the new std column (+ a col with stats of the standardization)
    import spark.implicits._

    def stdFuncAcc(df_data: DataFrame, df_vocIn: Option[DataFrame], ontoTagHierc: List[(String, (String, List[String], String, String))], prevVoc: String): DataFrame = {
      println("stdFuncAcc: " + ontoTagHierc)
      ontoTagHierc match {
        case Nil => df_data
        case x :: tail => {
          //Get Voc Name
          val vocName = x._2._3
          val vocCatMgr = CatalogMgr(auth = auth, ds_uri = vocName)

          //Load a new Voc or reuse the previous one
          val df_vocOpt: Option[DataFrame] = df_vocIn match {
            case Some(s) if vocName.equals(prevVoc) => Some(s)
            case _ => DatasetLoader.dataset_loadDF(vocName, spark)
          }
          println(vocName + " - " + df_vocOpt)
          //Match if a voc has been found, else pass the same dataframe (aka, don't do anything)
          df_vocOpt match {
            case Some(df_voc) => {
              //Nome della colonna da standardizzare
              //Step 1, inizio dalla prima in ordine di gerarchia
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
                  .filter(x=>x.startsWith(addedColVal))
                  .map(x => x.replace(addedColVal, "")).toList
                  .filter{x =>
                    val colTag = dataCatMgr.ontoTag.get(x)
                    colTag match {
                      case Some(s) => {
                        val endIndex = s.lastIndexOf(".")
                        val entity = s.substring(0,endIndex)
                        entityLinked.contains(entity)
                      }
                      case _ => false
                    }

                  }


              //Get the columns that need to be used in the df_data select (the col to be std and the correlated ones that have been already standardized)
              val colSelect_data: List[String] = colName :: colLinkedNames.toList
              val colSelect_voc: List[String] = colSelect_data.map{x=>
                val colTag = dataCatMgr.ontoTag.get(x)
                colTag match {
                  case Some(s) => vocCatMgr.ontoTag.find{x=>
//                    println(s"Here: $s - ${(x._2)}")
                    (x._2).equals(s)}.map(x=>x._1)
                  case _ => None
                }
              }.filter{x=>x.isDefined}.map{x=>x.get}
//              println(colName)
//              println(colSelect_data)
//              println(colSelect_voc)
//              println(dataCatMgr.ontoTag)
//              println(vocCatMgr.ontoTag)
              //get the table of the vocabulary to be used for standardization, with the columns needed for the matching and filter to restrict
              val voc = df_voc.select(colSelect_voc.head, colSelect_voc.tail: _*).distinct.rdd.collect()

              //produce the dataframe with the result of stdInference function (standardized col + score)
              val df_conv = df_data.select(colSelect_data.head, colSelect_data.tail: _*).distinct.rdd.map{x=>
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

              val df_data_joined = df_data.join(df_conv.toDF(colName, addedColVal + colName, addedColStat + colName), Seq(colName), "left")
              stdFuncAcc(df_data_joined, Some(df_voc), tail, vocName)
            }
            case _ => stdFuncAcc(df_data, None, tail, vocName)
          }
        }
      }

    }

    stdFuncAcc(df_data, None, ontoTagHierc, "-1")

  }

  //TODO in case we need to do different work for the streaming case
  def initStream(ds_uri: String, dataCatMgr: CatalogMgr, df_data: DataFrame): (Option[DataFrame], IngPipeReport)= initBatch(ds_uri, dataCatMgr, df_data)


}

/*
  //Given a ds_uri, it will start the ingestion pipeline and manage all spark work
  def init(ds_uri: String, spark: SparkSession): String = {

    val dataCatMgr = CatalogMgr(auth = "abcabc", ds_uri = ds_uri)

    val ontoMgr = OntoMgmt(dataCatMgr.ontoTag.map(x=>x._2).toList)
    val df_data = DatasetLoader.dataset_loadDF(ds_uri, spark)

    //[col_name, (semantic_tag, hierarchy, voc name, voc col)]
    val ontoTagHiercRaw: Seq[(String, (String, List[String], String, String))] = dataCatMgr.ontoTag.map{x =>
      x._1 -> (x._2, ontoMgr.getOntoInfoField(x._2).hierc, ontoMgr.getOntoInfoField(x._2).vocName, ontoMgr.getOntoInfoField(x._2).vocCol)
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

  def stdFunc(df_data: DataFrame, ontoTagHierc: List[(String, (String, List[String], String, String))], dataCatMgr: CatalogMgr, spark: SparkSession): DataFrame = {
    // Everytime the rec internal function is called, it produces a full standardization of a column
    // and return a new dataframe that join the initial with the new std column (+ a col with stats of the standardization)
    import spark.implicits._

    def stdFuncAcc(df_data: DataFrame, df_vocIn: Option[DataFrame], ontoTagHierc: List[(String, (String, List[String], String, String))], prevVoc: String): DataFrame = {
      ontoTagHierc match {
        case Nil => df_data
        case x :: tail => {
          //Get Voc Name
          val vocName = x._2._3
          val vocCatMgr = CatalogMgr(auth = "abcabc", ds_uri = vocName)

          //Load a new Voc or reuse the previous one
          val df_vocOpt: Option[DataFrame] = df_vocIn match {
            case Some(s) if vocName.equals(prevVoc) => Some(s)
            //case _ => ontoMgr.getDicDF(vocName, spark)
            case _ => DatasetLoader.dataset_loadDF(vocName, spark)
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
  def stdFunc2(df_data: DataFrame, ontoTagHierc: List[(String, (String, List[String], String, String))], dataCatMgr: CatalogMgr, spark: SparkSession): DataFrame = {
    // Everytime the rec internal function is called, it produces a full standardization of a column
    // and return a new dataframe that join the initial with the new std column (+ a col with stats of the standardization)
    import spark.implicits._

    def stdFuncAcc(df_data: DataFrame, df_vocIn: Option[DataFrame], ontoTagHierc: List[(String, (String, List[String], String, String))], prevVoc: String): DataFrame = {
      ontoTagHierc match {
        case Nil => df_data
        case x :: tail => {
          //Get Voc Name
          val vocName = x._2._3
          val vocCatMgr = CatalogMgr(auth = "abcabc", ds_uri = vocName)

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

*/