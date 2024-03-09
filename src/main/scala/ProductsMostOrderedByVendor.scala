import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.crealytics.spark.excel._
import breeze.plot._
import breeze.linalg._

object ProductsMostOrderedByVendor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Products Most Ordered By Vendor")
      .master("local[*]")
      .getOrCreate()

    // Assurez-vous que les chemins sont corrects pour votre environnement
    val ordersPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\data\\orders.csv"
    val vendorProductPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\data\\vendorproduct.csv"
    val vendorsPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\data\\vendors.csv"

    val ordersDF = spark.read.option("header", "true").option("inferSchema", "true").csv(ordersPath)
    val vendorProductDF = spark.read.option("header", "true").option("inferSchema", "true").csv(vendorProductPath)
    val vendorsDF = spark.read.option("header", "true").option("inferSchema", "true").csv(vendorsPath)

    // Agrégation des quantités commandées par ProductID
    val orderedProductsDF = ordersDF
      .groupBy("ProductID")
      .agg(org.apache.spark.sql.functions.sum("OrderQty").alias("TotalOrdered"))
      .orderBy(desc("TotalOrdered"))

    // Joindre les quantités commandées avec les informations sur les produits et les fournisseurs
    val productsMostOrderedByVendor = orderedProductsDF
      .join(vendorProductDF, "ProductID")
      .join(vendorsDF, "VendorID")
      .select("VendorID", "VendorName", "ProductID", "TotalOrdered")
      .orderBy(desc("TotalOrdered"))
      .limit(10)

    // Afficher les résultats
    productsMostOrderedByVendor.show()

    val outputPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\excel\\products_most_ordered_by_vendor.xlsx"


    val collectedData = productsMostOrderedByVendor.collect()

    // Préparation des données pour le graphique
    val vendorIDs = collectedData.map(_.getAs[Int]("VendorID").toDouble) // Conversion des VendorID en Double pour le graphique
    val totalOrdered = collectedData.map(_.getAs[Long]("TotalOrdered").toDouble) // TotalOrdered pour chaque VendorID

    val f = Figure()
    val p = f.subplot(0)

    // Utiliser les indices comme substitut pour les VendorID sur l'axe des abscisses
    val indices = DenseVector.rangeD(1, vendorIDs.length + 1, 1)
    val quantities = DenseVector(totalOrdered)

    // Créer un graphique de points pour représenter le total des commandes pour chaque VendorID
    p += plot(indices, quantities, '.', colorcode = "blue")



    p.title = "Total Orders per Vendor"
    p.xlabel = "Vendor ID"
    p.ylabel = "Total Ordered"

    f.refresh()
    f.saveas("total_orders_per_vendor_with_ids.png")

    productsMostOrderedByVendor
      .write
      .format("com.crealytics.spark.excel")
      .option("header", "true") // Pour inclure la ligne d'en-tête dans le fichier Excel
      .option("dataAddress", "'Sheet1'!A1") // Spécifie l'onglet et la cellule de départ (optionnel)
      .mode("overwrite") // Pour écraser un fichier existant
      .save(outputPath)

    spark.stop()
  }
}
