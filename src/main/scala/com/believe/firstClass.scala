package com.believe

import com.believe.keys.albumsKeys._
import com.believe.keys.salesKeys._
import com.believe.keys.songsKey._
import com.believe.readerUtils.readCsvWithHeader
import com.believe.utils.{applyOperations, enrichSalesByAlbums, enrichSalesBySongs, removeEmptyLines, removeNullLine}
import org.apache.spark.sql.functions.col

object firstClass {

  def main(args: Array[String]): Unit = {

    val sales = readCsvWithHeader("src/test/resources/sales.csv", ";")
    val songs = readCsvWithHeader("src/test/resources/songs.csv", ";")
    val albums = readCsvWithHeader("src/test/resources/albums.csv", ";")

    val salesEnrichedBySongs = enrichSalesBySongs(sales, songs)
    val salesEnriched = enrichSalesByAlbums(salesEnrichedBySongs, albums)

    val wantedOrder = Seq(ALBUM_UPC, SONG_ISRC, "label_name", "album_name", SONG_ID, "song_name", "artist_name", "content_type", "total_net_revenue", "sales_country")

    val operations = Seq(
      Rename(NET_TOTAL, "total_net_revenue"),
      Rename(TERRITORY, "sales_country"),
      Drop(PRODUCT_UPC),
      Drop(TRACK_ID),
      Drop(TRACK_ISRC_CODE),
      Drop(DELIVERY),
      Drop(ALBUM_COUNTRY),
      Reorder(wantedOrder),
      Cast("total_net_revenue", "double"),
      Cast(SONG_ID, "bigint")
    )

    val result = removeEmptyLines(removeNullLine(applyOperations(salesEnriched, operations)))

    result.printSchema
    result.show
  }
}
