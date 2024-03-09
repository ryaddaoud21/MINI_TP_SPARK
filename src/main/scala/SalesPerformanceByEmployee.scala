import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.crealytics.spark.excel._
import breeze.plot._
import breeze.linalg._

object SalesPerformanceByEmployee {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Sales Performance By Employee")
      .master("local[*]") // Exécution de Spark en local avec tous les cœurs disponibles
      .getOrCreate()

    // Chemins vers les fichiers
    val ordersPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\data\\orders.csv"
    val employeesPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\data\\employees.csv"

    // Charger les DataFrames
    val ordersDF = spark.read.option("header", "true").option("inferSchema", "true").csv(ordersPath)
    val employeesDF = spark.read.option("header", "true").option("inferSchema", "true").csv(employeesPath)

    // Joindre ordersDF avec employeesDF pour obtenir les détails des employés pour chaque commande
    val ordersEmployeesDF = ordersDF.join(employeesDF, ordersDF("EmployeeID") === employeesDF("EmployeeID"))

    // Calculer le montant total des ventes par employé et trier les résultats en ordre décroissant
    val salesByEmployeeDF = ordersEmployeesDF
      .groupBy(employeesDF("FullName")) // Assurez-vous que "FullName" est le nom correct de la colonne
      .agg(org.apache.spark.sql.functions.sum("TotalDue").alias("Total_Sales"))
      .orderBy(desc("Total_Sales")) // Ajout du tri en ordre décroissant

    // Afficher les résultats
    salesByEmployeeDF.show()


    val salesData = salesByEmployeeDF.collect()

    // Extraire les données pour le graphique
    val employeeNames = salesData.map(_.getAs[String]("FullName"))
    val totalSales = salesData.map(_.getAs[Double]("Total_Sales"))

    // Créer les indices pour chaque employé pour l'axe X du graphique
    val indices = DenseVector.rangeD(1, employeeNames.length + 1, 1)

    val f = Figure()
    val p = f.subplot(0)

    // Utiliser les indices comme approximation pour les EmployeeID sur l'axe des abscisses
    for (i <- indices.toArray.indices) {
      p += plot(DenseVector(i.toDouble + 1, i.toDouble + 1), DenseVector(0.0, totalSales(i)), '-', colorcode = "blue")
    }

    // Personnaliser le graphique
    p.title = "Total Sales by Employee"
    p.xlabel = "Employee Index"
    p.ylabel = "Total Sales"

    // Afficher le graphique
    f.refresh()

    // Sauvegarder le graphique si nécessaire
    f.saveas("total_sales_by_employee.png")
    f.saveas("C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\save\\total_sales_by_employee.png")


    val outputPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\excel\\sales_performance_by_employee.xlsx"
    salesByEmployeeDF
      .write
      .format("com.crealytics.spark.excel")
      .option("header", "true") // Pour inclure les en-têtes de colonne
      .option("dataAddress", "'Sheet1'!A1") // Spécifier l'onglet et la cellule de départ
      .mode("overwrite") // Pour écraser un fichier existant
      .save(outputPath)

    spark.stop()
  }
}
