package com.sonatel.yoonwi.launcher

import java.sql.Timestamp

import com.sonatel.yoonwi.classes.{Verifier, Inserter, Reader}
import com.sonatel.yoonwi.utils.CustomConfig
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.expressions.Window

//case class CarEvent(carId : Int, speed : Int, latitude : Double, longitude : Double, eventTime : Timestamp)
case class Event(i: Int, i1: Int, d: Double, d1: Double, timestamp: Timestamp)
object Checker {

  /*Mise en place d'un logger*/
  val logger : Logger = LogManager.getLogger(getClass)

  val hadoopConfig = new Configuration()

  def main(args:Array[String]):Unit={

    /**
      * Verifying number of arguments in the command line :
      * Check if configuration file is provided
      */
    if(args.length == 0){
      logger.info("Le programme requiert un fichier de configuration pour s'exécuter")
      System.exit(0)
    }

    /**
      * Retrieving configuration file directory
      */
    val hdpConfigPath = args(0)

    /**
      * Loading configuration file
      */
    CustomConfig.load(hdpConfigPath+"/application.conf")

    /**
      * Retrieving configuration parameters values
      */
    val STREAM_TOPIC_NAME=CustomConfig.STREAM_TOPIC_NAME
    val BOOTSTRAP_SERVERS=CustomConfig.BOOTSTRAP_SERVERS
    val DEFAULT_TRONCONS_PATH=CustomConfig.DEFAULT_TRONCONS_PATH
    val SPARK_WAREHOUSE_PATH=CustomConfig.SPARK_WAREHOUSE_PATH

    /*Mongo DB parameters*/
    val OUTPUT_COLLECTION = CustomConfig.OUTPUT_COLLECTION
    val OUTPUT_URI = CustomConfig.OUTPUT_URI

    /**
      * Spark Session
      */
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("YOONWI")
      .config("spark.sql.warehouse.dir",SPARK_WAREHOUSE_PATH)
      //Writing into mongo db collection
      .config("spark.mongodb.output.uri", OUTPUT_URI)
      .config("spark.mongodb.output.collection", OUTPUT_COLLECTION)
      .getOrCreate()


      /**
      * Reading troncons file
      */
      val troncons : DataFrame = new Reader(spark, DEFAULT_TRONCONS_PATH).read()

     //for compiler error relating to $
    //Schema of the streaming dataframe
   /* val schema = StructType(Array(
      StructField("id", IntegerType),
      StructField("date",StringType),
      StructField("heure",StringType),
      StructField("vitesse", IntegerType),
      StructField("latitude",DoubleType),
      StructField("longitude",DoubleType)
    ))*/
    /**
      * Creating a Kafka Source for Streaming Queries into vehicles topic
      */
    val dataframeStreaming = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
      .option("subscribe", STREAM_TOPIC_NAME)
      .option("startingOffsets","earliest")
      .load()
    /*Adding Time Column : processing time window*/

    import spark.implicits._

  //  case class CarEvent(carId : Option[Int], speed : Option[Int], latitude : Option[Double], longitude : Option[Double], eventTime : Timestamp)

    object CarEvent{
      def apply(rawStr : String):Event = {
        val parts = rawStr.split(",")
        Event(Integer.parseInt(parts(1)),Integer.parseInt(parts(0)), java.lang.Double.parseDouble(parts(2)), java.lang.Double.parseDouble(parts(3)), new Timestamp(parts(4).toLong))
      }
    }

    val cars = dataframeStreaming
      .selectExpr("CAST(value AS STRING)")
      .map(r => CarEvent(r.getString(0))).toDF("carId","speed","latitude","longitude","eventTime")
   // cars.show(5)

    /*val cars = dataframeStreaming
      .select($"key" cast "string", //deserialize key
        $"value" cast "string",
        $"topic",
        $"partition",
        $"offset"
      )*/

    //val data = cars.writeStream.format("console").start()

    /*Method : using Window Function
    //val windowSpec = Window($"timestamp", "1 minute", "16 seconds")
    */
    /*Using groupBy function
    //cars.rdd.groupBy(window($"timestamp", "1 minute","16 seconds"))
   */

    /**
      * Retrieving all columns with join
      */
     // val resultSet = cars.groupBy(window($"eventTime","1 minute"), $"carId").agg(max($"speed"))//.writeStream.format("console").start()

   // val agg = cars.groupBy(window($"eventTime","1 minute","16 seconds"), $"speed", $"carId", $"latitude", $"longitude").agg(first($"speed") as "speed")
     // val joinedDS = cars.join(agg, "carId")

    val aggregates = cars.groupBy(window($"eventTime","1 minute","16 seconds"), $"speed").agg(first($"speed") as "carId", first($"carId") as "speed", first($"latitude") as "latitude", first($"longitude") as "longitude")



   // val query = agg.writeStream.outputMode("complete").format("console").option("truncate", "false").start//complete, append, update mode


    val q = aggregates.writeStream.outputMode("complete").format("console").option("truncate", "false").start

    q.awaitTermination()
   // query.awaitTermination()


    //val windowSpec = Window.partitionBy("eventTime", "1 minute")

    //val result = cars.withColumn("maxSpeed", max($"speed").over(windowSpec))



  //  resultSet.stop()

  //  joinedDS.show(5)

    /*Retrieving all columns with first()*/
  /*  val agg = cars.groupBy(window($"timestamp","1 minute","16 seconds"), $"carId").agg(first($"carId") as "car")
    agg.show(5)

    val joinedAgg = cars.join(agg, "carId")


       Retrieving all columns with join another example

      val data_counts = cars.groupBy(window($"timestamp","1 minute","16 seconds"), $"carId").agg(count($"carId"))

    val joinedDF = cars.join(data_counts, $"carId")

    joinedDF.show(5)

    a tumbling window of size 10 seconds
    val aggregates : DataFrame= cars.groupBy(window($"timestamp", "1 minute", "16 seconds"), $"carId").agg(sum($"carId")) //cars.grouBy(window($"timestamp","1 minute","16 seconds"), $"carId").agg(first($"carId" as "car")

    aggregates.show(5)
*/
    /**
      * Creating a Kafka Source for Batch Queries : inutile
      */
    /*val dataframeBatch = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
      .option("subscribe", BATCH_TOPIC_NAME)
      .load()
    dataframeBatch.isStreaming
    dataframeBatch.show(5)

    case class Troncon(tronconId : Option[Int], voie : Option[Int], latitudePO : Option[Double], longitudePO : Option[Double],
                       latitudeDO : Option[Double], longitudeDO : Option[Double], latitudePE : Option[Double],
                       longitudePE : Option[Double], latitudeDE : Option[Double], longitudeDE : Option[Double],longueur : Option[Int] , observation : Option[Int],
                       vLibre : Option[Int], vCritique : Option[Int], dCritique : Option[Double])
    object Troncon{
      def apply(rawStr : String) : Troncon={
        val parts = rawStr.split(",")

        Troncon(Some(Integer.parseInt(parts(0))), Some(Integer.parseInt(parts(1))), Some(java.lang.Double.parseDouble(parts(2))),
          Some(java.lang.Double.parseDouble(parts(3))),Some(java.lang.Double.parseDouble(parts(4))),Some(java.lang.Double.parseDouble(parts(5))),
          Some(java.lang.Double.parseDouble(parts(6))),Some(java.lang.Double.parseDouble(parts(7))),Some(java.lang.Double.parseDouble(parts(8))),
          Some(java.lang.Double.parseDouble(parts(9))),Some(Integer.parseInt(parts(10))),Some(Integer.parseInt(parts(11))),
          Some(Integer.parseInt(parts(12))), Some(Integer.parseInt(parts(13))), Some(java.lang.Double.parseDouble(parts(14))))
      }
    }
    //val troncons = dataframeBatch.selectExpr("CAST(key AS STRING)","CAST(value AS STRING")
    val troncons = dataframeBatch.selectExpr("CAST (value AS STRING)")
      .map(r => Troncon(r.getString(0))).toDF("tronconId","voie","latitudePO","longitudePO","latitudeDO", "longitudeDO", "latitudePE", "longitudePE", "latitudeDE","longitudeDE", "longueur", "observation", "vLibre", "vCritique", "dCritique")
    troncons.show(5)

    //val etatsTroncons : DataFrame = new Checker(spark, troncons, cars).check()
*/

      /*Appel de la classe Checker et déduction des états des tronçons*/
    /*  val etats : DataFrame = new Checker(spark, troncons, joinedDF).check()

      /*Ajout du temps de traitement*/
      val etatsDates : DataFrame = etats.withColumn("date", lit(System.currentTimeMillis()))

    /**
      * Inserting into MongoDB : not yet
      */
      val inserter = new Inserter(OUTPUT_URI, OUTPUT_COLLECTION, etatsDates)

      inserter.insert()*/

  }

}


