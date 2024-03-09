import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import breeze.plot._
import breeze.linalg._
import org.apache.spark.sql.Row

object OrdersPerMonth {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Orders Per Month")
      .master("local[*]")
      .getOrCreate()


     val ordersDF = spark.read.option("header", "true").csv("C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\data\\orders.csv")

    val ordersWithDate = ordersDF
      .withColumn("OrderDate", to_date(col("OrderDate"), "M/d/yyyy"))

    // Extraire l'année et le mois de la date de commande
    val ordersWithMonthYear = ordersWithDate
      .withColumn("Year", year(col("OrderDate")))
      .withColumn("Month", month(col("OrderDate")))

    // Compter les commandes par mois et année
    val ordersCountPerMonth = ordersWithMonthYear
      .groupBy("Year", "Month")
      .agg(count("SalesOrderID").alias("NumberOfOrders"))
      .orderBy(desc("Year"), desc("Month"))

    // Afficher les résultats
    ordersCountPerMonth.show()
    val ordersData = ordersCountPerMonth.orderBy("Year", "Month").collect()


    val monthsIndices = DenseVector.rangeD(1, ordersData.length + 1, 1)
    val ordersCount = DenseVector(ordersData.map(_.getAs[Long]("NumberOfOrders").toDouble): _*)

    val f = Figure()
    val p = f.subplot(0)

    // Créer un graphique en ligne
    p += plot(monthsIndices, ordersCount, '-', colorcode = "blue")
    p += plot(monthsIndices, ordersCount, '-', colorcode = "red")

    // Personnaliser le graphique
    p.title = "Nombre de Commandes par Mois"
    p.xlabel = "Mois (Index)"
    p.ylabel = "Nombre de Commandes"

    // Afficher le graphique
    f.refresh()

    // Sauvegarder le graphique
    f.saveas("C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\save\\orders_per_month_line_graph.png")

    // Chemin de sortie pour le fichier Excel
    val excelOutputPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\excel\\orders_per_month.xlsx"

    // Exporter le DataFrame au format Excel
    ordersCountPerMonth
      .write
      .format("com.crealytics.spark.excel")
      .option("header", "true") // Pour inclure les noms de colonnes comme en-tête
      .option("dataAddress", "'Sheet1'!A1") // Nom de l'onglet et position de début dans le fichier Excel
      .mode("overwrite") // Écrase le fichier s'il existe déjà
      .save(excelOutputPath)

    spark.stop()
  }
}
