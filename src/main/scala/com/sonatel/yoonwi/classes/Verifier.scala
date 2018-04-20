package com.sonatel.yoonwi.classes

import com.sonatel.yoonwi.utils.GPS
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class Verifier(spark : SparkSession, troncons:DataFrame, vehicules : DataFrame){

  troncons.createOrReplaceTempView("troncon")
  vehicules.createOrReplaceTempView("vehicule")

  /*UDF pour vérifier la présence du point ou véhicule à l'intérieur du tronçon : winding number*/
  val contains=(vLatitude : Double, vLongitude : Double, latitudePO : Double,
                longitudePO : Double, latitudePE : Double, longitudePE : Double,
                latitudeDE : Double, longitudeDE : Double, latitudeDO : Double, longitudeDO : Double)  =>{

    def isLeft(point1 : GPS, point2 : GPS, v : GPS): Int = {

      /*tests if a point is Left|On|Right of an infinite line(p0,p1)
    * > 0 for p2 left of the line through p0 and p1
    * = 0 for p2 on the line
    * < 0 for p2 right of the line*/
      val det = (point2.latitude - point1.latitude) * (v.longitude- point1.longitude) - (v.latitude-point1.latitude) * (point2.longitude - point1.longitude)
      if (det < 0) -1
      else if (det > 0) +1
      else 0
    }
    var troncon = List[GPS]()
    val vehicule = new GPS(vLatitude, vLongitude)
    var windings = 0 //compteur de l'indice

    val po = new GPS(latitudePO, longitudePO)
    val pe = new GPS(latitudePE, longitudePE)
    val de = new GPS(latitudeDE, longitudeDE)
    val dorigine = new GPS(latitudeDO, longitudeDO)
    //constitution de la liste
    troncon = po :: troncon
    troncon = pe :: troncon
    troncon = de :: troncon
    troncon = dorigine :: troncon
    //Fermeture du polygone : le dernier point doit être égale au premier point du polygone
    troncon = po :: troncon
    for (i <- 0 to troncon.size - 2) {
      val ccwResult = isLeft(troncon(i), troncon(i+1), vehicule)

      if(troncon(i+1).longitude > vehicule.longitude && vehicule.longitude >= troncon(i).longitude) {
        //upward crossing
        if (ccwResult == +1) {
          windings += 1
        }
      }
      if(troncon(i+1).longitude <= vehicule.longitude && vehicule.longitude < troncon(i).longitude){
        //downward crossing
        if(ccwResult == -1){
          windings -= 1
        }
      }
    }
    windings
  }
  /*Registering udf*/
  spark.sqlContext.udf.register("winding", contains)

  def check(): DataFrame ={

    /**
      * Récupération de densites
      */
   val  densites = spark.sql("SELECT t.tronconId as troncon, t.voie as voies, t.longueur as longueur, t.capacite as capacite, t.concentrationCritique as concentrationCritique, t.count(*) as nbVehicules, " +
     "t.count(*)/t.longueur as dActuel, t.vitesseLibre as vLibre, t.vitesseCritique as vCritique, avg(v.vitesse) as vMoyenne FROM troncon t, vehicule v" +
     "WHERE winding(v.latitude, v.longitude, t.latitudePO, t.longitudePO, t.latitudePE, t.longitudePE, t.latitudeDE, t.longitudeDE, t.latitudeDO, t.longitudeDO) <> 0 ")

    densites.createOrReplaceTempView("densites")
    /**
      * Déduction des états des tronçons
      */
    val etatsFuildes : DataFrame= spark.sql("SELECT d.troncon, d.voie, d.longueur, d.nbVehicules, d.vLibre, d.vCritique, d.vMoyenne, d.capacite, d.concentrationCritique FROM densites d" +
      "WHERE d.dActuel > 0  AND d.vMoyenne >= d.vLibre")
    val tronconsFluides = etatsFuildes.withColumn("etat",lit("Fluide"))

    val etatsNormaux : DataFrame = spark.sql("SELECT d.troncon, d.voie, d.longueur, d.nbVehicules, d.vLibre, d.vCritique, d.vMoyenne d.capacite, d.concentrationCritique FROM densites d" +
      "WHERE d.dActuel > 0 AND d.dActuel <= d.concentrationCritique AND d.vMoyenne < d.vLibre AND d.vMoyenne >= d.vCritique")
    val tronconsNormaux = etatsNormaux.withColumn("etat",lit("Trafic"))

    val etatsCongestiones = spark.sql("SELECT d.troncon, d.voie, d.longueur, d.nbVehicules, d.vLibre, d.vCritique, d.vMoyenne d.capacite, d.concentrationCritique FROM densites d" +
      "WHERE d.dActuel >= d.concentrationCritique AND d.dActuel < d.capacite AND d.vMoyenne < d.vCritique")
    val tronconsCongestiones = etatsCongestiones.withColumn("etat", lit("Congestion"))

    val etatsEmbouteilles = spark.sql("SELECT d.troncon, d.voie, d.longueur, d.nbVehicules, d.vLibre, d.vCritique, d.vMoyenne d.capacite, d.concentrationCritique FROM densites d" +
      "WHERE d.dActuel = d.capacite AND d.vMoyenne = 0")
    val tronconsBouchons = etatsEmbouteilles.withColumn("etat",lit("Bouchon"))

    val etatsVides = spark.sql("SELECT d.troncon, d.voie, d.longueur, d.nbVehicules, d.vLibre, d.vCritique, d.vMoyenne FROM densites d" +
      "WHERE d.actuel = 0 AND d.vMoyenne = 0")
    val tronconsVides = etatsVides.withColumn("etat", lit("Vide"))

    val etatsTroncons = tronconsFluides.union(tronconsNormaux).union(tronconsCongestiones).union(tronconsBouchons).union(tronconsVides)

    /*Return les états des différents tronçons*/
    etatsTroncons
  }

}
