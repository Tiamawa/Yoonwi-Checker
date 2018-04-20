package com.sonatel.yoonwi.classes

import com.mongodb.spark._
import org.apache.spark.sql.DataFrame

class Inserter(output_uri : String, output_collection : String, etats : DataFrame){


	case class Etat (troncon : Int, voie : Int, longueur : Int, nbVehicules : Int, vLibre : Int, vCritique : Int, vMoyenne : Int, etat : String)
	def insert() : Unit = {

		MongoSpark.save(etats.write.mode("overwrite"))

	}



}
