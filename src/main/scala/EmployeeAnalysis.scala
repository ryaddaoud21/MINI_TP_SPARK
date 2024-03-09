import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import breeze.linalg._

object EmployeeAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Employee Analysis")
      .master("local[*]")
      .getOrCreate()

    val employeesPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\data\\employees.csv"

    val employeesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(employeesPath)

    // Compter les employés par région (Territory) et pays (Country)
    val employeesByRegionAndCountry = employeesDF
      .groupBy("Territory", "Country")
      .agg(count("*").alias("NumberOfEmployees"))
      .orderBy("Territory", "Country")

    // Afficher le résultat
    employeesByRegionAndCountry.show()

    val collectedData = employeesByRegionAndCountry.collect()
    val territoriesCountries = collectedData.map(row => row.getAs[String]("Territory") + ", " + row.getAs[String]("Country"))
    val numberOfEmployees = collectedData.map(row => row.getAs[Long]("NumberOfEmployees").toDouble)

    import breeze.plot._

    val f = Figure()
    val p = f.subplot(0)
    val x = DenseVector.rangeD(1, territoriesCountries.length + 1, 1)
    val y = new DenseVector(numberOfEmployees)

    // Dessiner des lignes verticales pour simuler des barres
    for (i <- x.toArray) {
      p += plot(Array(i, i), Array(0.0, y(i.toInt - 1)), '-', colorcode = "blue")
    }

    p.xlabel = "Territories and Countries"
    p.ylabel = "Number of Employees"
    p.title = "Number of Employees by Territory and Country"

    f.refresh()
    f.saveas("C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\save\\employees_by_territory_and_country.png")


    employeesByRegionAndCountry
      .write
      .format("com.crealytics.spark.excel")
      .option("header", "true") // Pour inclure la ligne d'entête dans le fichier Excel
      .option("dataAddress", "'Sheet1'!A1") // Nom de la feuille et cellule de départ
      .mode("overwrite") // Pour écraser un fichier existant
      .save("C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\excel\\employees_by_region_country.xlsx")

    spark.stop()
  }
}
