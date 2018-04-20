package com.sonatel.yoonwi.classes

import org.apache.spark.sql.{DataFrame, SparkSession}

class Reader(spark : SparkSession, filePath : String){

	def read() : DataFrame = {

		val troncons = spark.read
					.option("header", "true")
					.option("delimiter", ";")
					.csv(filePath)

		troncons
	}
}