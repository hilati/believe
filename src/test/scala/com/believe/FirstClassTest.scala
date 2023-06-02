package com.believe

import com.believe.ReaderUtils.{readCsvWithHeader, readCsvWithHeaderAndSchema}
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.types._

class FirstClassTest extends AnyFlatSpec with DataFrameComparer with SparkTestSession with Matchers{

  "First class test" should "return the expected dataframe" in {
    // Given
    val sales = readCsvWithHeader("src/test/resources/salesTest.csv", ";")
    val songs = readCsvWithHeader("src/test/resources/songsTest.csv", ";")
    val albums = readCsvWithHeader("src/test/resources/albumsTest.csv", ";")

    // expected elements
    val expectedSchema = StructType(Array(
      StructField("upc", StringType, true),
      StructField("isrc", StringType, true),
      StructField("label_name", StringType, true),
      StructField("album_name", StringType, true),
      StructField("song_id", LongType, true),
      StructField("song_name", StringType, true),
      StructField("artist_name", StringType, true),
      StructField("content_type", StringType, true),
      StructField("total_net_revenue", DoubleType, true),
      StructField("sales_country", StringType, true)
    ))
    val expectedDF = readCsvWithHeaderAndSchema("src/test/resources/expectedTest.csv", ";", expectedSchema)

    val result = new FirstClass(sales,songs, albums).result

    //compare
    result.schema.map(_.name) should contain allOf(expectedDF.columns.head,expectedDF.columns.tail.head, expectedDF.columns.tail.tail:_*)
    result.count() shouldBe expectedDF.count
    assertSmallDataFrameEquality(result, expectedDF)

  }

}