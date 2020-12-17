/* Delhaye François : francois.delhaye@isen.yncrea.fr */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.apache.log4j.{Level, Logger}

object UsAirports {

  /**
   * UsAirports Spark application
   * Complete the code to provided, you can used the slides of the course
   *
   * @param args
   */

  def main(args: Array[String]):Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)  // Efface toutes les informations fournies lors de l'éxécution hormi les erreurs
    //TODO Define the spark session (application name is UsAirports)
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("UsAirports")
      .getOrCreate()

    //TODO Read the file Airports.csv (downloaded from teams)
    val AirportsDF = spark.read.option("header",value = true).csv("C:/Users/Delhaye/Desktop/Airports.csv")
    println("AirportsDF : ")
    AirportsDF.show(5)
    //TODO Read the file UsAirportsInfos.csv (also downloaded from teams)
    val UsAirportsInfosDF = spark.read.option("header",value = true).csv("C:/Users/Delhaye/Desktop/UsAirportsInfos.csv")
    println("UsAirportsInfosDF : ")
    UsAirportsInfosDF.show(5)

    //TODO Display the number of line contained in AirportsDF
    AirportsDF.count()
    println("Display the number of line contained in AirportsDF : " + AirportsDF.count()) // Réponse :  3606803
    //TODO Display the number of lines in UsAirportsInfosDF
    UsAirportsInfosDF.count()
    println("Display the number of lines in UsAirportsInfosDF : " + UsAirportsInfosDF.count() ) // Réponse : 23653
    //TODO Add the Destination_airport_name to the Airports DataFrame, name it AirportsJoinDF.
    //TODO AirportsJoinDF must only contains all the columns of AirportsDF + Destination_airport_name
    //tip : you can choose between two columns of UsAirportsInfosDF to perform the join with AirportsDF
    val UsAirportsInfosDFToJoin = UsAirportsInfosDF.select("iata_code","name") // on sélectionne les colonnes du second tableau à joindre
    val AirportsJoinDF = AirportsDF.join(UsAirportsInfosDFToJoin, AirportsDF("Destination_airport") === UsAirportsInfosDF("iata_code"))
      .withColumnRenamed("name","Destination_airport_name").drop("iata_code")
    println("Add the Destination_airport_name to the Airports DataFrame : ")
    AirportsJoinDF.show(5)
    //TODO Apply the same transformation to add the Origin_airport_name information
    val AirportsJoinDF2 = AirportsDF.join(UsAirportsInfosDFToJoin, AirportsDF("Origin_airport") === UsAirportsInfosDFToJoin("iata_code"))
      .drop("iata_code").withColumnRenamed("name","Origin_airport_name")
    println("Apply the same transformation to add the Origin_airport_name information : ")
    AirportsJoinDF2.show(5)
    //TODO How many flights have "Jefferson City Memorial Airport" as their origin
    //tip : you can use groupBy
    val numFlights1 = AirportsJoinDF2.filter("Origin_airport_name = 'Jefferson City Memorial Airport'").count()  // Réponse : 6506
    println("How many flights have Jefferson City Memorial Airport as their origin ? " + numFlights1)
    //TODO Display the names (and the counts) of the 5 most frequent destination airports (descending order)
    //tip : you can also use groupBy
    //tip : the Dataset scaladoc is available here : https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html
    val mostFrequentDest = AirportsJoinDF.groupBy("Destination_airport_name").count().sort(desc("count"))
    println("Display the names of the 5 most frequent destination airports (descending order) : ")
    mostFrequentDest.show(5)
    //TODO Written like that the code may not be optimized, what can you do to improve it ?

    // Le temps d'éxécution est de 20 s (garbage collection : 0.6 s) pour 226 Tasks. Pour optimiser le code, on va utiliser la mémoire grâce à l'option
    // .cache() qui va stocker la variable (ou les variables) en mémoire ou sur un disque afin que les mêmes opérations ne soient rééxécutées
    // plusieurs fois

    //tip : you can check the Spark UI at localhost:4040
    //TODO Add the optimizations to the code above

    //If the application is shut down, you can no longer access to the Spark UI
    //This infinite loop allow you to access to the Spark UI
    //Shut down the application manually when you want to update your code or
    // when you are done with it
    while(true) {
      Thread.sleep(1000)
    }
  }

}
