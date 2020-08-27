package com.precisely.bigdata.spectrum.global.spark.mapMarker

object MatchType extends Enumeration{

  type main = Value

  val Address = Value(10, "Address")
  val Street = Value(8, "Street")
  val Admin = Value(6, "Admin")
  val POBox = Value(4, "POBox")
  val NoMatch = Value(0, "NoMatch")

   def withNameWithDefault(precisionCode: String): Value = {
    if (isAddressSeries(precisionCode)) {
      Address;
    }
    else if (isStreetSeries(precisionCode)) {
      Street;
    }
    else if (isAdminSeries(precisionCode)) {
      Admin;
    }
    else if (precisionCode.contains("B")) {
      POBox;
    }else{
      NoMatch;
    }    
  }

  def isAddressSeries(precisionCode: String): Boolean ={
    Seq("S8H", "S7H", "S6H", "S5H", "S3H", "S2H", "S1H", "S0H", "SX")
      .exists(code => precisionCode.contains(code))
  }

  def isStreetSeries(precisionCode: String): Boolean ={
    Seq("S8-", "S7-", "S6-", "S5-", "S3-", "S2-", "S1-", "S4")
      .exists(code => precisionCode.contains(code))
  }

  def isAdminSeries(precisionCode: String): Boolean ={
    Seq("G3", "G4", "G1", "G2", "Z3", "Z2", "Z1", "SL","SG")
      .exists(code => precisionCode.contains(code))
  }
}
