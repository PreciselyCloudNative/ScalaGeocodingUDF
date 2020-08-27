package com.precisely.bigdata.spectrum.global.spark.file

import org.apache.spark.sql.{DataFrame, SparkSession}

object FileInput {
  def open(session: SparkSession, filePath: String, format: String, numPartitions: Integer): DataFrame = {
    format match {
      case "csv" => session.read.option("header", "true").option("delimiter", ",").format("csv").load(filePath).repartition(numPartitions)
      case "tsv" => session.read.option("header", "true").option("delimiter", "\t").format("csv").load(filePath).repartition(numPartitions)
      case "psv" => session.read.option("header", "true").option("delimiter", "|").format("csv").load(filePath).repartition(numPartitions)
      case _ => session.read.format(format).load(filePath).repartition(numPartitions)
    }
  }
}
