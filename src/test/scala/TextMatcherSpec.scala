package it.gov.daf.ingestion.std.it.gov.daf.ingestion.std.lib

import org.specs2._

class TextMatcherSpec extends mutable.Specification{

  "TextMatcher should: " should {
    "return a List of tuple: " in {
      //DataMock.getData("ds_luoghi").getClass().toString must beEqualTo("class scala.collection.immutable.$colon$colon")
      TextMatcher.doit("levenshtein")("pirla", Seq("pirla", "prla", "palla", "cazzo")) must beAnInstanceOf[(String, String, String)]
    }
    "return score zero if words are the same: " in {
      //DataMock.getData("ds_luoghi").getClass().toString must beEqualTo("class scala.collection.immutable.$colon$colon")
      TextMatcher.doit("levenshtein")("pirla", Seq("pirla", "prla", "palla", "cazzo"))._3 must beEqualTo(0)
    }

  }


}
