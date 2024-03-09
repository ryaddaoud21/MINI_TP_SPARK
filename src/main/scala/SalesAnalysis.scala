import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.crealytics.spark.excel._
import breeze.plot._
import breeze.linalg._

object SalesAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Sales Analysis")
      .master("local[*]") // Exécution de Spark en local avec tous les cœurs disponibles
      .getOrCreate()

    val ordersPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\data\\orders.csv"
    val productsPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\data\\products.csv"

    val ordersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(ordersPath)

    val productsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(productsPath)

    // Calculer les ventes totales par ProductID
    val salesSummaryDF = ordersDF
      .groupBy("ProductID")
      .agg(
        org.apache.spark.sql.functions.sum("OrderQty").alias("Total_Quantity_Sold"),
        org.apache.spark.sql.functions.sum("LineTotal").alias("Total_Revenue")
      )

    // Joindre salesSummaryDF avec productsDF pour inclure ProductName dans le résultat
    val salesSummaryWithProductNameDF = salesSummaryDF
      .join(productsDF, "ProductID")
      .select("ProductID", "ProductName", "Total_Quantity_Sold", "Total_Revenue")

    salesSummaryWithProductNameDF.show()

    val collectedData = salesSummaryWithProductNameDF.collect()

    // Extraire les données pour le graphique
    val productNames = collectedData.map(_.getAs[String]("ProductName"))
    val totalQuantitiesSold = collectedData.map(_.getAs[Long]("Total_Quantity_Sold").toDouble)
    val totalRevenues = collectedData.map(_.getAs[Double]("Total_Revenue"))

    // Indices pour l'axe X
    val indices = DenseVector.rangeD(1, productNames.length + 1, 1)

    // Créer un graphique pour Total_Quantity_Sold
    val f1 = Figure()
    val p1 = f1.subplot(0)
    p1 += plot(indices, DenseVector(totalQuantitiesSold), '.', colorcode = "blue")
    p1.title = "Total Quantity Sold by Product"
    p1.xlabel = "Product Index"
    p1.ylabel = "Total Quantity Sold"
    f1.refresh()
    f1.saveas("C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\save\\total_quantity_sold_by_product.png")

    // Créer un graphique pour Total_Revenue
    val f2 = Figure()
    val p2 = f2.subplot(0)
    p2 += plot(indices, DenseVector(totalRevenues), '.', colorcode = "red")
    p2.title = "Total Revenue by Product"
    p2.xlabel = "Product Index"
    p2.ylabel = "Total Revenue"
    f2.refresh()
    f2.saveas("C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\save\\total_revenue_by_product.png")

    val outputPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\excel\\sales_summary.xlsx"

    salesSummaryWithProductNameDF
      .write
      .format("com.crealytics.spark.excel")
      .option("header", "true") // Pour inclure la ligne d'en-tête
      .option("dataAddress", "'Sheet1'!A1") // Spécifie l'onglet et la cellule de départ
      .mode("overwrite") // Pour écraser un fichier existant
      .save(outputPath)

    spark.stop()
  }
}
