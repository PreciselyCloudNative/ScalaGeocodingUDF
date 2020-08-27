package com.precisely.bigdata.spectrum.global.spark.file

import org.rogach.scallop.exceptions.{Help, Version}
import org.rogach.scallop.{ArgType, ScallopConf, ScallopOption, ValueConverter}

class BaseCommandLine(arguments: Seq[String], driverClass: String, inputFieldsHelpAddition: String) extends ScallopConf(arguments) {
  version("spark-submit --class " + driverClass + " --master yarn --deploy-mode cluster [SPARK_SUBMIT_OPTIONS] <JAR_PATH> <JOB_OPTIONS>")

  val input: ScallopOption[String] = opt[String](descr = "The HDFS or s3 path to the input directory or file", required = true, noshort = true)
  val output: ScallopOption[String] = opt[String](descr = "The HDFS or s3 path to the output directory", required = false, noshort = true)
  val format: ScallopOption[String] = opt[String](descr = "The format of input file,csv tsv, psv, parquet or orc", required = true, noshort = true)
  val numPartitions: ScallopOption[Int] = opt[Int](descr = "Number of partitions to use for processing input", noshort = true)
  val longitude: ScallopOption[String] = opt[String](descr = "Longitude column of input file", required = false, noshort = true)
  val latitude: ScallopOption[String] = opt[String](descr = "Latitude column of input file", required = false, noshort = true)
  val downloadLocation: ScallopOption[String] = opt[String](descr = "Location of the local directory where reference data will be downloaded to", noshort = true)

  val geocodingResourcesLocation: ScallopOption[String] = opt[String](descr = "The resources location for Geocoding Library", required = true, noshort = true)
  val geocodingInputFields: Map[String, String] = propsLong[String](descr = "List of input fields mapped to request fields. " + inputFieldsHelpAddition, name = "geocoding-input-fields")(new CommaAllowedConverter)
  val geocodingPreferencesFilepath: ScallopOption[String] = opt[String](descr = "Path of the geocoding preferences file", noshort = true)
  val geocodingOutputFields: ScallopOption[List[String]] = opt[List[String]](descr = "Fields from the geocoding candidate to include in the output", noshort = true)
  val geocodingCountry: ScallopOption[String] = opt[String](descr = "Country to use for all records when input country is not specified or is empty", noshort = true)
  val fallback: ScallopOption[String] = opt[String](descr = "fallback search distance in feet", required = false, noshort = true)
  val searchDistance: ScallopOption[Int] = opt[Int](descr = "reverse geocode search distance in feet", required = false, noshort = true)

  val liDataLocation: ScallopOption[String] = opt[String](descr = "The data location for li boundaries", required = false, noshort = true)
  val boundaryFilename: ScallopOption[String] = opt[String](descr = "Name of boundary file", noshort = true)
  val radius: ScallopOption[Int] = opt[Int](descr = "Radius for nearest search", noshort = true)
  val numCandidate: ScallopOption[Int] = opt[Int](descr = "Maximum number of candidates to return", noshort = true)

  val dynamoFields: Map[String, String] = propsLong[String](name = "dynamo-fields", descr = "List of dynamoDB fields. tableName, primaryKey, writeCapacity, readCapacity")(new CommaAllowedConverter)

  //we want to print usage out on general error
  override def onError(e: Throwable): Unit = e match {
    case Version =>
      super.onError(e)
    case Help("") =>
      super.onError(e)
    case _ =>
      printHelp()
      super.onError(e)
  }

  def validateArgs(): Unit = {
    if (input.isEmpty) {
      throw new IllegalArgumentException("Required option 'input' not found")
    }
  }
}


class GeocodingCommandLine(arguments: Seq[String], driverClass: String) extends BaseCommandLine(arguments, driverClass, "Input fields should be address field, such as mainAddressLine, postCode1, etc..") {
  override def validateArgs(): Unit = {
    super.validateArgs()
    if (format.isEmpty) {
      throw new IllegalArgumentException("Required option 'format' not found")
    }
  }

  verify()
  validateArgs()
}

//This custom converter allows comma to be embedded without escaping, and is limited to string values.
private class CommaAllowedConverter extends ValueConverter[Map[String, String]] {
  override val argType: ArgType.V = org.rogach.scallop.ArgType.LIST

  override def parse(s: List[(String, List[String])]): Either[String, Option[Map[String, String]]] = {
    try {
      Right {
        val pairs = s.flatMap(_._2).map(_.trim)
        val m = pairs.map { pair =>
          val kv = pair.split("(?<!\\\\)=").map(_.replace("\\=", "="))
          (kv(0), kv(1))
        }.toMap

        if (m.nonEmpty) Some(m)
        else None
      }
    } catch {
      case _: Exception =>
        Left("wrong arguments format")
    }
  }
}


