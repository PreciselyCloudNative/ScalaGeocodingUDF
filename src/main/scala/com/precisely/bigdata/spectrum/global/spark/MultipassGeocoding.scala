package com.precisely.bigdata.spectrum.global.spark

import com.pb.bigdata.geocoding.spark.api.GeocodeUDFBuilder
import com.precisely.bigdata.spectrum.global.spark.mapMarker.GeocodingAccumulator
import com.precisely.bigdata.spectrum.global.spark.file.{DriverUtils, FileInput, GeocodingCommandLine}
import com.precisely.bigdata.spectrum.global.spark.mapMarker.MultipassGeocodeExecutor
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable

object MultipassGeocoding {

  private val COUNTRY_KEY = "country"
  case class GeocodingJobResults(val totalResults: Long, val successRecord: Long, val failedRecords: Long,
                                 val premiumCount: Long, val basicCount: Long)
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
    else List("X", "Y", "formattedStreetAddress", "formattedLocationAddress", "PrecisionCode", "areaName3",
      "areaName1", "postCode1")


    val sparkConf = new SparkConf()
    sparkConf.setIfMissing("spark.master", "local[*]")
    val session = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // Load the addresses from the csv
    val addressInputDF = FileInput.open(session, inputPath, format, repartition)
    val accumulator = GeocodingAccumulator()
    session.sparkContext.register(accumulator, "Geocoding Stats")

    // build a singleCandidateUDF, with custom GeocodeExecutor
      GeocodeUDFBuilder.singleCandidateUDFBuilder()
        .withGeocodeExecutor(new MultipassGeocodeExecutor(accumulator))
        .withResourcesLocation(resourcesLocation)
        .withPreferencesFile(preferenceLocation)
        .withDownloadLocation(downloadLocation)
        .withOutputFields(outputFields: _*)
        .withErrorField("error")
        .register("geocode", session)

    // call UDF for each row in the address DataFrame
    // this will result in the dataframe containing a new column for each of the specified
    // output fields as well as an error column
    addressInputDF
      // Adds a new column, represented as a collection comprised of the outputFields and the error field
      .withColumn("geocode_result", callUDF("geocode", buildInputAddressMap(commandLine,addressInputDF)
    )
    )
      // Persist the geocode result to avoid recalculation when we expand the result
      .persist()
      // Expand the result collection such that each output field is a separate column, including the error field.
      .select("*", "geocode_result.*").drop("geocode_result")
      // Write the dataframe to the specified output folder as parquet file
      .repartition(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").option("delimiter", ",").save(outputPath + "/outputFiles")

    val accumulatedResult:mutable.HashMap[String, Long]  = accumulator.value

    val successRecordCount:Long = accumulatedResult.get(GeocodingAccumulator.SUCCESS).getOrElse(0)
    val failedRecordCount:Long = accumulatedResult.get(GeocodingAccumulator.FAILURE).getOrElse(0)
    val totalRecordCount:Long = successRecordCount + failedRecordCount
    val premiumCount:Long = accumulatedResult.get(GeocodingAccumulator.PREMIUM).getOrElse(0)
    val basicCount:Long = accumulatedResult.get(GeocodingAccumulator.BASIC).getOrElse(0)

    import session.implicits._
    Seq(GeocodingJobResults(totalRecordCount, successRecordCount, failedRecordCount, premiumCount, basicCount))
      .toDF
      .repartition(1)
      .write
      .mode("overwrite")
      .option("header", true)
      .csv(outputPath + "/stats")
  }
  private val buildInputAddressMap = (commandLine: GeocodingCommandLine, df: DataFrame) => {
    //country has an option to be specified separately on the command line, so we have special handling here for that
    map(commandLine.geocodingInputFields.filterNot(_._1 == COUNTRY_KEY)
      //map them to pairs of literal for address field name and concatenated (when multiple columns were specified) list of columns from input df
      .map(inputField => (lit(inputField._1), concat_ws(" ", inputField._2.split(",").map(field => col(df.columns(DriverUtils.getRequiredColumnIndex(inputField._1, Option(field), df)))): _*)))
      //flatten the tuple into a list of columns for the map constructor
      .flatMap(tuple => Seq(tuple._1, tuple._2))
      .toList :+
      //add in the custom country definition
      lit("country") :+
      DriverUtils.buildFallbackToLiteral("country", commandLine.geocodingCountry, df, commandLine): _*)
  }
}