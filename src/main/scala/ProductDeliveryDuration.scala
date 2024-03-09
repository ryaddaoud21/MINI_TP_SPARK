import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.crealytics.spark.excel._
import breeze.plot._
import breeze.linalg._


object ProductDeliveryDuration {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Product Delivery Duration")
      .master("local[*]")
      .getOrCreate()

    val ordersPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\data\\orders.csv"
    val ordersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(ordersPath)
      .withColumn("OrderDate", to_date(col("OrderDate"), "M/d/yyyy"))
      .withColumn("ShipDate", to_date(col("ShipDate"), "M/d/yyyy"))

    // Calculer la durée de livraison pour chaque ligne de commande
    val deliveryDurationDF = ordersDF
      .withColumn("DeliveryDuration", datediff(col("ShipDate"), col("OrderDate")))

    // Calculer la durée moyenne de réception pour chaque produit
    val avgDeliveryDurationByProduct = deliveryDurationDF
      .groupBy("ProductID")
      .agg(avg("DeliveryDuration").alias("AverageDeliveryDuration"))
      .orderBy("ProductID")

    avgDeliveryDurationByProduct.show()


    // Supposons que avgDeliveryDurationByProduct est déjà calculé
    val collectedData = avgDeliveryDurationByProduct.collect()

    // Préparer les données pour la visualisation
    val productIDs = collectedData.map(row => row.getAs[Int]("ProductID").toDouble) // IDs des produits
    val avgDeliveryDurations = collectedData.map(row => row.getAs[Double]("AverageDeliveryDuration")) // Durées moyennes

    // Créer les vecteurs pour les axes X et Y
    val x = DenseVector(productIDs)
    val y = DenseVector(avgDeliveryDurations)

    // Créer et configurer le graphique en ligne
    val f = Figure()
    val p = f.subplot(0)
    p += plot(x, y, '-', colorcode = "blue")
    p.title = "Average Delivery Duration by Product"
    p.xlabel = "Product ID"
    p.ylabel = "Average Delivery Duration (days)"

    // Afficher la figure
    f.refresh()
    f.saveas("C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\save\\average_delivery_duration_by_product.png")


    val outputPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\excel\\avg_delivery_duration_by_product.xlsx"
    avgDeliveryDurationByProduct
      .write
      .format("com.crealytics.spark.excel")
      .option("header", "true") // Inclure les entêtes de colonne
      .option("dataAddress", "'Sheet1'!A1") // Spécifier l'onglet et la cellule de début
      .mode("overwrite") // Écraser le fichier existant
      .save(outputPath)

    spark.stop()
  }
}
