package com.believe

sealed trait Operation

case class Rename(existingColumn: String, expectedColumn: String) extends Operation

case class Drop(toDropColumn: String) extends Operation

case class Cast(toCastColumn: String, typeExpected: String) extends Operation

case class Reorder(wantedOrderColumn: Seq[String]) extends Operation