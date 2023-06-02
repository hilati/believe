package com.believe

import org.apache.spark.sql.DataFrame
import com.believe.SparkTestSession._
import org.apache.spark.sql.types.StructType

object ReaderUtils {
  def readCsvWithHeader(path: String, separator: String): DataFrame = {
    spark.read.option("header", "true")
      .option("delimiter", separator)
      .csv(path)
  }

  def readCsvWithHeaderAndSchema(path: String, separator: String, schema: StructType): DataFrame = {
    spark.read.option("header", "true")
      .option("delimiter", separator)
      .schema(schema)
      .csv(path)
  }

}
