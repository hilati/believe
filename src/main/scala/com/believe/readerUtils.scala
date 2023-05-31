package com.believe

import org.apache.spark.sql.DataFrame
import com.believe.SparkTestSession._

object readerUtils {
  def readCsvWithHeader(path: String, separator: String): DataFrame = {
    spark.read.option("header", "true")
      .option("delimiter", separator)
      .csv(path)
  }
}
