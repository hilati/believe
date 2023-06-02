package com.believe

import com.believe.Keys.albumsKeys._
import com.believe.Keys.salesKeys._
import com.believe.Keys.songsKey._
import com.believe.ReaderUtils.readCsvWithHeader
import com.believe.Utils.{applyOperations, enrichSalesByAlbums, enrichSalesBySongs, removeEmptyLines, removeNullLine}
import org.apache.spark.sql.DataFrame

class FirstClass (sales:DataFrame, songs:DataFrame, albums:DataFrame) {
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
}
object FirstClass {

  def main(args: Array[String]): Unit = {
    val sales = readCsvWithHeader("src/main/resources/sales.csv", ";")
    val songs = readCsvWithHeader("src/main/resources/songs.csv", ";")
    val albums = readCsvWithHeader("src/main/resources/albums.csv", ";")

    val result = new FirstClass(sales, songs, albums).result

    //just to show result for this test
    result.printSchema
    result.show
  }
}
