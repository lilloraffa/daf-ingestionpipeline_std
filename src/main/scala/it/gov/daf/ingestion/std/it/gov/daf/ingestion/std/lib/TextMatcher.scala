package it.gov.daf.ingestion.std.it.gov.daf.ingestion.std.lib

import scala.math.min

object TextMatcher {

  def doit(matchType: String)(stringIn: String, voc: Seq[String]): (String, String, Int) = {
    matchType match {
      case "levenshtein" => levenshtein(stringIn, voc)
      case _ => levenshtein(stringIn, voc)
    }
  }

  def levenshtein(stringIn: String, voc: Seq[String]): (String, String, Int) = {

    def editDist[A](a: Iterable[A], b: Iterable[A]) = {
      ((0 to b.size).toList /: a)((prev, x) =>
        (prev zip prev.tail zip b).scanLeft(prev.head + 1) {
          case (h, ((d, v), y)) => min(min(h + 1, v + 1), d + (if (x == y) 0 else 1))
        }) last
    }
    //get list of tuple (stringIn, stringTrans, score)
    val scorList = voc.map(x=> (stringIn, x, editDist(stringIn, x))).sortBy(_._3)
    scorList(0)

  }

}
