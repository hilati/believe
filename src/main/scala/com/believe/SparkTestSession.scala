package com.believe

import org.apache.spark.sql.SparkSession

object SparkTestSession {
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Spark SQL basic example")
    .getOrCreate()

}
