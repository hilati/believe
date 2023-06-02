package com.believe

import org.apache.spark.sql.SparkSession

trait SparkTestSession {
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Spark SQL basic example")
    .getOrCreate()

}
