package com.believe


import org.apache.spark.sql._
import com.believe.keys._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object utils {
  def enrichSalesBySongs(sales: DataFrame, songs: DataFrame) = {
    val expression = sales(salesKeys.TRACK_ISRC_CODE) === songs(songsKey.SONG_ISRC) && sales(salesKeys.TRACK_ID) === songs(songsKey.SONG_ID)
    sales.join(songs, expression, "inner")
  }

  def enrichSalesByAlbums(sales: DataFrame, albums: DataFrame) = {
    val expression = sales(salesKeys.PRODUCT_UPC) === albums(albumsKeys.ALBUM_UPC) && sales(salesKeys.TERRITORY) === albums(albumsKeys.ALBUM_COUNTRY)
    sales.join(albums, expression, "inner")
  }

  def f(df: DataFrame, operation: Operation) = {
    operation match {
      case Rename(existingColumn, expectedColumn) =>
        df.withColumnRenamed(existingColumn, expectedColumn)
      case Drop(toDropColumn) =>
        df.drop(toDropColumn)
      case Reorder(wantedOrderColumn) =>
        df.select(wantedOrderColumn.head, wantedOrderColumn.tail: _*)
      case Cast(toCastColumn: String, typeExpected: String) =>
        df.withColumn(toCastColumn, df.col(toCastColumn).cast(typeExpected))
      case _ => df
    }
  }

  def applyOperations(dataFrame: DataFrame, operations: Seq[Operation]): DataFrame = {
    operations.foldLeft(dataFrame)(f)
  }

  def removeNullLine(df: DataFrame) = df.na.drop


  def removeEmptyLines(df: DataFrame) = df.columns.foldLeft(df)((df, column) => df.filter(col(column) =!= "" || col(column).getClass.toString != "String"))
}
