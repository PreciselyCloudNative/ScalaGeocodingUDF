package com.precisely.bigdata.spectrum.global.spark.mapMarker

object LocationType extends Enumeration{

  type main = Value

  val S8H = Value(100, "S8H")
  val SX = Value(99, "SX")
  val S7H = Value(95, "S7H")
  val S5H = Value(90, "S5H")
  val S6H = Value(89, "S6H")
  val S3H = Value(85, "S3H")
  val S8 = Value(80, "S8-")
  val S7 = Value(79, "S7-")
  val S6 = Value(77, "S6-")
  val S5 = Value(75, "S5-")
  val S4H = Value(70, "S4H")
  val S4 = Value(69, "S4-")
  val S3 = Value(60, "S3-")
  val SL = Value(59, "SL")
  val S2H = Value(50, "S2H")
  val S2 = Value(49, "S2-")
  val S1H = Value(48, "S1H")
  val S1 = Value(47, "S1-")
  val G4 = Value(45, "G4")
  val SG = Value(44, "SG")
  val Z3 = Value(40, "Z3")
  val G3 = Value(39, "G3")
  val G2 = Value(20, "G2")
  val Z1 = Value(19, "Z1")
  val Z2 = Value(18, "Z2")
  val B1 = Value(15, "B1")
  val B2 = Value(14, "B2")
  val G1 = Value(10, "G1")
  val NoMatch = Value(0, "NoMatch")


  def withNameWithDefault(name: String): Value ={
    values.find(_.toString.toLowerCase == normalizeName(name).toLowerCase()).getOrElse(NoMatch)
  }

  def normalizeName(name: String): String= {
    if(name.length >= 3)
      name.substring(0, 3)
    else
      name
  }
}
