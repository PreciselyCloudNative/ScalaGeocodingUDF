package com.precisely.bigdata.spectrum.global.spark.mapMarker

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * [SUCCESS -> 10
  * FAILURE -> 1]
  *
  * @param accValue
  */
case class GeocodingAccumulator(var accValue: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]())
  extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

  override def isZero: Boolean = accValue.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = GeocodingAccumulator(accValue)

  override def reset(): Unit = accValue = new mutable.HashMap[String, Long]()

  override def add(recordType: String): Unit = {
    val nextVal = if (accValue.contains(recordType)) accValue(recordType) + 1 else 1
    accValue += (recordType -> nextVal)
  }

  def update(recordType: String, value: Long) : Unit = {
    val nextVal = if(accValue.contains(recordType)) accValue(recordType) + value else value
    accValue += (recordType -> nextVal)
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    for (x <- other.value) {
      update(x._1, x._2)
    }
  }

  override def value: mutable.HashMap[String, Long] = accValue

  @Override
  override def toString(): String =
    "[" +
      GeocodingAccumulator.SUCCESS + "->" + accValue.get(GeocodingAccumulator.SUCCESS)

  "," + GeocodingAccumulator.FAILURE + "->" + accValue.get(GeocodingAccumulator.FAILURE) +
    "]"

}

object GeocodingAccumulator {
  val SUCCESS: String = "SUCCESS"
  val FAILURE: String = "FAILURE"
  val BASIC: String = "BASIC"
  val PREMIUM: String = "PREMIUM"
}