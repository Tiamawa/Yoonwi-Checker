package com.sonatel.yoonwi.utils

import java.io.File

import com.typesafe.config.ConfigFactory

object CustomConfig {

  //Kafka parameters
  var STREAM_TOPIC_NAME=""
  var BOOTSTRAP_SERVERS=""
  var AVERAGE_RADIUS_EARTH=0

  //Mongo
  var OUTPUT_URI=""
  var OUTPUT_COLLECTION=""

  //HDFS
  var DEFAULT_TRONCONS_PATH=""
  var SPARK_WAREHOUSE_PATH = ""

  def load(inputPath : String): Unit ={

    val myConfigFile=new File(inputPath)
    val configFile = ConfigFactory.parseFile(myConfigFile)

    STREAM_TOPIC_NAME=configFile.getString("YOONWI.KAFKA.STREAM_TOPIC_NAME")
    BOOTSTRAP_SERVERS=configFile.getString("YOONWI.KAFKA.BOOTSTRAP_SERVERS")
    DEFAULT_TRONCONS_PATH=configFile.getString("YOONWI.HDFS.DEFAULT_TRONCONS_PATH")
    SPARK_WAREHOUSE_PATH=configFile.getString("YOONWI.HDFS.SPARK_WAREHOUSE_PATH")
    OUTPUT_URI=configFile.getString("YOONWI.MONGO.OUTPUT_URI")
    OUTPUT_COLLECTION=configFile.getString("YOONWI.MONGO.OUTPUT_COLLECTION")
  }
}
