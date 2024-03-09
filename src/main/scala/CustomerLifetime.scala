import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import breeze.plot._
import breeze.linalg._

object CustomerLifetime {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Customer Lifetime Duration")
      .master("local[*]")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")

      .getOrCreate()
    import spark.implicits._

    val ordersPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\data\\orders.csv"

    val ordersDF = spark.read.option("header", "true").option("inferSchema", "true").csv(ordersPath)

    val processedOrdersDF = ordersDF
      .withColumn("OrderDate", to_date(col("OrderDate"), "MM/dd/yyyy"))

    // Calculer la première et la dernière date de commande pour chaque client
    val customerLifetimesDF = processedOrdersDF
      .groupBy("CustomerID")
      .agg(
        org.apache.spark.sql.functions.min("OrderDate").alias("DatePremièreCommande"),
        org.apache.spark.sql.functions.max("OrderDate").alias("DateDernièreCommande")
      )
      .withColumn("DuréeVieClient", datediff(col("DateDernièreCommande"), col("DatePremièreCommande")))

    // Afficher le résultat
    customerLifetimesDF.show()

    // Calculer et afficher la durée de vie moyenne du client
    val averageLifetime = customerLifetimesDF.agg(avg("DuréeVieClient")).first().get(0)
    println(s"Durée de vie moyenne du client: $averageLifetime jours")

    // Collecter les données de la durée de vie des clients
    val lifetimesData = customerLifetimesDF.select($"DuréeVieClient").as[Int].collect()

    // Création d'indices pour chaque client (pour l'axe X)
    val indices = DenseVector.rangeD(1, lifetimesData.length + 1, 1)
    // Conversion des données de durée de vie en DenseVector pour l'axe Y
    val lifetimes = DenseVector(lifetimesData.map(_.toDouble))

    val f = Figure()
    val p = f.subplot(0)

    // Créer un graphique en ligne
    p += plot(indices, lifetimes, '-', colorcode = "blue")

    // Personnaliser le graphique
    p.title = "Durée de Vie des Clients"
    p.xlabel = "Indice du Client"
    p.ylabel = "Durée de Vie (jours)"

    // Afficher le graphique
    f.refresh()

    f.saveas("C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\save\\customer_lifetime_histogram.png")

    customerLifetimesDF
      .write
      .format("com.crealytics.spark.excel")
      .option("header", "true") // Pour inclure la ligne d'entête dans le fichier Excel
      .option("dataAddress", "'Sheet1'!A1") // Nom de la feuille et cellule de départ
      .mode("overwrite") // Pour écraser un fichier existant
      .save("C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\excel\\customer_life_times.xlsx")
    spark.stop()
  }
}
