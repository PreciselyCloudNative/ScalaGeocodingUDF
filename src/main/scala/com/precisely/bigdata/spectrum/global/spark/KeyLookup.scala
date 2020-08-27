package com.precisely.bigdata.spectrum.global.spark

import com.pb.bigdata.geocoding.spark.api.{GeocodeByKeyUDFBuilder, GeocodeUDFBuilder}
import com.precisely.bigdata.spectrum.global.spark.file.{FileInput, GeocodingCommandLine}
import com.precisely.bigdata.spectrum.global.spark.geocodingUserDefineFunction.MultipassGeocodeExecutor
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object KeyLookup {
  def main(args: Array[String]): Unit = {
    val commandLine = new GeocodingCommandLine(args, this.getClass.getName)
    val inputPath = commandLine.input()
    val outputPath = commandLine.output()
    val format = commandLine.format()
    val repartition: Int = if (commandLine.numPartitions.isDefined) commandLine.numPartitions() else 1
    val downloadLocation = if (commandLine.downloadLocation.isDefined) commandLine.downloadLocation() else "/mnt/pb/downloads"
    val resourcesLocation = commandLine.geocodingResourcesLocation()
    val preferenceLocation = commandLine.geocodingPreferencesFilepath()
    val outputFields = if (commandLine.geocodingOutputFields.isDefined) commandLine.geocodingOutputFields()
    else List("X", "Y", "formattedStreetAddress", "formattedLocationAddress", "PrecisionCode", "PB_KEY", "areaName3",
      "areaName1", "postCode1")


    val sparkConf = new SparkConf()
    sparkConf.setIfMissing("spark.master", "local[*]")
    val session = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // Load the addresses from the csv
    var addressInputDF = FileInput.open(session, inputPath, format, repartition)

    // build a singleCandidateUDF, with custom GeocodeExecutor
    GeocodeByKeyUDFBuilder.singleCandidateUDFBuilder()
      .withResourcesLocation(resourcesLocation)
      .withPreferencesFile(preferenceLocation)
      .withDownloadLocation(downloadLocation)
      .withOutputFields(outputFields: _*)
      .withErrorField("error")
      .register("keylookup", session)

    addressInputDF = addressInputDF.withColumn("pbkey", regexp_replace(col("PB_KEY"), "X0000", "P0000"))
      .drop("PB_KEY")
    // call UDF for each row in the address DataFrame
    // this will result in the dataframe containing a new column for each of the specified
    // output fields as well as an error column
    addressInputDF
      // Adds a new column, represented as a collection comprised of the outputFields and the error field
      .withColumn("keylookup_result", callUDF("keylookup", col("pbkey"), lit("PB_KEY"), lit("USA"))
    )
      // Persist the geocode result to avoid recalculation when we expand the result
      .persist()
      // Expand the result collection such that each output field is a separate column, including the error field.
      .select("*", "keylookup_result.*").drop("keylookup_result")
      // Write the dataframe to the specified output folder as parquet file
      .repartition(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").option("delimiter", ",").save(outputPath)
  }
}