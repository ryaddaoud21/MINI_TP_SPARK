import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.crealytics.spark.excel._
import breeze.plot._
import breeze.linalg._

object TopSellingProducts {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Top Selling Products")
      .master("local[*]")
      .getOrCreate()

    val ordersPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\data\\orders.csv"
    val productsPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\data\\products.csv"

    val ordersDF = spark.read.option("header", "true").option("inferSchema", "true").csv(ordersPath)
    val productsDF = spark.read.option("header", "true").option("inferSchema", "true").csv(productsPath)

    val topSellingProductsDF = ordersDF
      .groupBy("ProductID")
      .agg(org.apache.spark.sql.functions.sum("OrderQty").alias("Total_Quantity_Sold"))
      .join(productsDF, "ProductID")
      .select("ProductID","ProductName", "Total_Quantity_Sold")
      .orderBy(desc("Total_Quantity_Sold"))
      .limit(10)

    topSellingProductsDF.show()

    // Collecter les données pour les 10 produits les plus vendus
    val collectedTop10 = topSellingProductsDF.collect()
    val productIDsTop10 = collectedTop10.map(row => row.getAs[Int]("ProductID").toDouble)
    val quantitiesSoldTop10 = collectedTop10.map(row => row.getAs[Long]("Total_Quantity_Sold").toDouble)

    // Visualisation avec Breeze
    val f = Figure()
    val p = f.subplot(0)

    val indices = DenseVector.rangeD(1, productIDsTop10.length + 1, 1)
    val quantities = new DenseVector(quantitiesSoldTop10)

    // Simulation d'un graphique à barres avec des points pour chaque produit
    for (i <- indices.toArray) {
      p += plot(Array(i, i), Array(0.0, quantities(i.toInt - 1)), '-', colorcode = "blue")
    }

    p.xlabel = "Index des Produits (Top 10)"
    p.ylabel = "Quantités Vendues"
    p.title = "Simulation d'un Graphique à Barres pour les 10 Produits les Plus Vendus"

    f.saveas("C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\save\\top_10_selling_products_bar_simulation.png")

    val outputPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\excel\\top_selling_products.xlsx"

    topSellingProductsDF
      .write
      .format("com.crealytics.spark.excel")
      .option("header", "true") // Pour inclure les noms de colonnes comme en-têtes
      .option("dataAddress", "'Sheet1'!A1") // Spécifie l'onglet et la cellule de départ
      .mode("overwrite") // Pour écraser un fichier existant
      .save(outputPath)

    spark.stop()
  }
}
