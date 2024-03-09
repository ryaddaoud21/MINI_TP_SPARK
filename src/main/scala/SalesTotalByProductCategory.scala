import org.apache.spark.sql.SparkSession
import breeze.plot._
import breeze.linalg._



object SalesTotalByProductCategory {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Sales Total By Product Category")
      .master("local[*]")
      .getOrCreate()

    // Charger les DataFrames
    val ordersPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\data\\orders.csv"
    val productsPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\data\\products.csv"
    val productCategoriesPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\data\\productcategories.csv"
    val productSubcategoriesPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\data\\productsubcategories.csv"

    val ordersDF = spark.read.option("header", "true").option("inferSchema", "true").csv(ordersPath)
    val productsDF = spark.read.option("header", "true").option("inferSchema", "true").csv(productsPath)
    val productCategoriesDF = spark.read.option("header", "true").option("inferSchema", "true").csv(productCategoriesPath)
    val productSubcategoriesDF = spark.read.option("header", "true").option("inferSchema", "true").csv(productSubcategoriesPath)

    // Joindre les DataFrames
    val productsSubcategoriesDF = productsDF.join(productSubcategoriesDF, "SubCategoryID")
    val productsCategoriesDF = productsSubcategoriesDF.join(productCategoriesDF, "CategoryID")
    val ordersProductsCategoriesDF = ordersDF.join(productsCategoriesDF, "ProductID")

    // Calculer les revenus totaux par catégorie de produit
    val revenueByCategoryDF = ordersProductsCategoriesDF
      .groupBy(productCategoriesDF("CategoryID"))
      .agg(org.apache.spark.sql.functions.sum("LineTotal").alias("Total_Revenue"))


    // Afficher les résultats
    revenueByCategoryDF.show()

    // Collecter les résultats en mémoire
    val collectedData = revenueByCategoryDF.collect()

    // Préparation des données pour le graphique
    val categoryIDs = collectedData.map(_.getAs[Int]("CategoryID").toDouble) // Convertir les CategoryID en Double
    val totalRevenues = collectedData.map(_.getAs[Double]("Total_Revenue")) // Revenus totaux pour chaque catégorie

    val f = Figure()
    val p = f.subplot(0)

    // Simuler un graphique à barres en utilisant des lignes verticales pour chaque catégorie
    val barWidth = 0.2 // Définir la largeur des barres simulées
    for (i <- categoryIDs.indices) {
      val x = Array.fill(2)(categoryIDs(i))
      val y = Array(0.0, totalRevenues(i)) // De 0 au revenu total pour la catégorie
      p += plot(DenseVector(x), DenseVector(y), '-', colorcode = "blue")
    }

    // Personnaliser le graphique
    p.title = "Total Revenue by Product Category"
    p.xlabel = "Category ID"
    p.ylabel = "Total Revenue"
    p.xlim(0.0, categoryIDs.max + 1) // Ajuster les limites de l'axe X

    // Afficher et sauvegarder le graphique
    f.refresh()

    f.saveas("C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\save\\revenue_by_category.png")











    val outputPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\excel\\revenue_by_category.xlsx"

    revenueByCategoryDF
      .write
      .format("com.crealytics.spark.excel")
      .option("header", "true") // Pour inclure les noms de colonnes comme en-têtes
      .option("dataAddress", "'Sheet1'!A1") // Spécifie l'onglet et la cellule de départ
      .mode("overwrite") // Pour écraser un fichier existant
      .save(outputPath)


    spark.stop()
  }
}
