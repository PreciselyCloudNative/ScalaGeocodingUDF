/*
 * Copyright 2017, 2021 Precisely. All rights reserved.
 * This document contains unpublished, confidential, and proprietary information of Precisely.
 * No disclosure or use of any portion of the contents of this document may be made without the express written consent of Precisely.
 */

package com.precisely

import com.precisely.addressing.v1.model.{FactoryDescriptionBuilder, PreferencesBuilder}
import com.precisely.bigdata.addressing.spark.api.{AddressingBuilder, UDFBuilder}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes.DoubleType

import scala.annotation.switch
import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, mapAsScalaMapConverter}
import scala.collection.convert.ImplicitConversions.`map AsScala`


object Addressing {
  private val COUNTRY_KEY = "country"
  val country = "USA"

  def addressingDF(inputDF: DataFrame, operation: String, ResourcesLocationLocal: String, ExtractLocationLocal: String, DataLocationLocal: java.util.List[String],
                   outputFields: java.util.List[String], inputFields: java.util.Map[String, String]): DataFrame = {

    val udfBuilder: UDFBuilder = new AddressingBuilder()
      .withResourcesLocation(ResourcesLocationLocal)
      .withDataLocations(DataLocationLocal.asScala.toList: _*)
      .withExtractionLocation(ExtractLocationLocal)
      .udfBuilder()
      .withErrorField("error")
      .withOutputFields(outputFields.asScala.toList: _*)

    val operationUdf: UserDefinedFunction = (operation: @switch) match {
      //initialize underline JVM more than once
      case "multipass" =>
        udfBuilder.forCustomExecutor(new CustomExecutor())
      case "geocode" =>
        udfBuilder.withPreferences(new PreferencesBuilder()
          .withReturnAllInfo(true)
          .withCustomPreference("GEOHASH_LEVEL", "6")
          .withCustomPreference("FIND_STREET_CENTROID", "false")
          .withCustomPreference("FIND_DPV", "true")
          .withCustomPreference("FIND_LACSLINK", "true")
          .withCustomPreference("FIND_RDI", "true")
          .build)
          .forGeocode()
      case "verify" =>
        udfBuilder.withPreferences(new PreferencesBuilder()
          .withReturnAllInfo(true)
          .withFactoryDescription(new FactoryDescriptionBuilder().withLabel("finalist").build)
          .withCustomPreference("SCORE_THRESHOLD", "0")
          .withCustomPreference("VM_DATA_BLOCK", "N")
          .withCustomPreference("EARLY_WARNING_SYSTEM", "true")
          .withCustomPreference("R777_DELIVERABLE", "true")
          .withCustomPreference("REMOVE_NOISE_CHARS", "true")
          .withCustomPreference("RETURN_ALIAS_STREET_WITH_TYPE", "ON")
          .withCustomPreference("RETURN_INPUT_FIRM", "true")
          .withCustomPreference("RETURN_SLK_SECONDARY", "B")
          .withCustomPreference("STAND_ALONE_PMB", "N")
          .withCustomPreference("STAND_ALONE_UNIT", "N")
          .withCustomPreference("FINALIST_LOG_LEVEL", "2")
          .build)
          .forVerify()
      //   parse is removed untill it is re-introduced.
      //			case "parse" =>
      //				udfBuilder.forParse()
      case "lookup" =>
        udfBuilder.withPreferences(new PreferencesBuilder()
          .withReturnAllInfo(true)
          .withCustomPreference("GEOHASH_LEVEL", "6")
          .withCustomPreference("FIND_STREET_CENTROID", "false")
          .withCustomPreference("FIND_DPV", "true")
          .withCustomPreference("FIND_LACSLINK", "true")
          .withCustomPreference("FIND_RDI", "true")
          .build)
          .forLookup()
      case "reverseGeocode" =>
        udfBuilder.withPreferences(new PreferencesBuilder()
          .withReturnAllInfo(true)
          .withCustomPreference("GEOHASH_LEVEL", "6")
          .withCustomPreference("FIND_STREET_CENTROID", "false")
          .withCustomPreference("FIND_DPV", "true")
          .withCustomPreference("FIND_LACSLINK", "true")
          .withCustomPreference("FIND_RDI", "true")
          .build)
          .forReverseGeocode()
      case _ =>
        throw new IllegalArgumentException("Not a valid '--operation' parameter")
    }

    val datasetName = "addressing_result"
    val outputDF: DataFrame = (operation: @switch) match {
      case "geocode" | "verify" | "parse" | "multipass" =>
        inputDF
          // Adds a new column, represented as a collection comprised of the outputFields and the error field
          .withColumn(datasetName, operationUdf(
            buildInputAddressMap(inputFields.asScala.toMap, inputDF)
          ))
          // Persist the addressing result to avoid recalculation when we expand the result
          .persist()
          // Expand the result collection such that each output field is a separate column, including the error field.
          .select("*", datasetName + ".*").drop(datasetName)
      case "lookup" =>
        inputDF.withColumn(datasetName, operationUdf(lit("PB_KEY"), col(inputDF.columns(Integer.valueOf(inputFields.get("key")))), lit("USA")))
          .persist
          .select("*", datasetName + ".*").drop(datasetName)

      case "reverseGeocode" =>
        if(inputFields.contains("country"))
          inputDF.withColumn(datasetName, operationUdf(col(inputDF.columns(Integer.valueOf(inputFields.get("x")))).cast(DoubleType), col(inputDF.columns(Integer.valueOf(inputFields.get("y")))).cast(DoubleType), col(inputDF.columns(Integer.valueOf(inputFields.get("country"))))))
            .persist
            .select("*", datasetName + ".*").drop(datasetName)
        else
          inputDF.withColumn(datasetName, operationUdf(col(inputDF.columns(Integer.valueOf(inputFields.get("x")))).cast(DoubleType), col(inputDF.columns(Integer.valueOf(inputFields.get("y")))).cast(DoubleType), lit("USA")))
            .persist
            .select("*", datasetName + ".*").drop(datasetName)
      case _ =>
        throw new IllegalArgumentException("Not a valid 'operation' parameter")
    }
    outputDF
  }

  private val buildInputAddressMap = (inputFields: Map[String, String], df: DataFrame) => {
    //country has an option to be specified separately on the command line, so we have special handling here for that
    map(inputFields.filterNot(_._1 == COUNTRY_KEY)
      //map them to pairs of literal for address field name and concatenated (when multiple columns were specified) list of columns from input df
      .map(inputField => (lit(inputField._1), concat_ws(" ", inputField._2.split(",").map(field => col(df.columns(DriverUtils.getRequiredColumnIndex(inputField._1, Option(field), df)))): _*)))
      //flatten the tuple into a list of columns for the map constructor
      .flatMap(tuple => Seq(tuple._1, tuple._2))
      .toList :+
      //add in the custom country definition
      lit("country") :+
      DriverUtils.buildFallbackToLiteral("country", country, df, inputFields): _*)
  }
}

//import org.apache.spark.sql.api.java.UDF1
//import com.precisely.Addressing.{addressingUdf, session}
//class sparkDemoFunction3 extends UDF1[String,String]{
//  override def call(fullAddress: String):String ={
//    fullAddress
//  }
//}
//object StringLength {
//  def getStringLength(s: String) = s.length
//  def getFun(): UserDefinedFunction = udf(getStringLength _)
//}
//class sparkDemoFunction extends UDF1[String,String]{
//  override def call(unit_des: String):String ={
//    if(unit_des=="APT" || unit_des=="UNIT" || unit_des=="#"){
//      return "unit"
//    }
//    else{
//      return "i don't know"
//    }
//  }
//}
