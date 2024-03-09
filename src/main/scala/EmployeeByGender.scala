import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.crealytics.spark.excel._
import breeze.plot._
import breeze.linalg._

object EmployeeByGender {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Employee By Gender")
      .master("local[*]")
      .getOrCreate()

    val employeePath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\data\\employees.csv"

    val employeeDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(employeePath)

    // Compter les employés par genre
    val countByGender = employeeDF
      .groupBy("Gender")
      .agg(count("*").alias("NumberOfEmployees"))
      .orderBy("Gender")

    // Afficher le résultat
    countByGender.show()
    // Chemin où sauvegarder le fichier Excel
    val outputPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\excel\\employees_by_gender.xlsx"


    // Collecter les données pour la visualisation
    val collectedData = countByGender.collect()

    // Préparer les données pour l'histogramme
    val genderLabels = collectedData.map(_.getString(0)) // Genre : Male, Female, etc.
    val numberOfEmployees = collectedData.map(_.getLong(1).toDouble) // Nombre d'employés pour chaque genre

    val f = Figure()
    val p = f.subplot(0)

    // Utiliser un vecteur d'indices comme approximation pour les genres
    val indices = DenseVector.rangeD(1, genderLabels.length + 1, 1)
    val quantities = new DenseVector(numberOfEmployees)

    // Dessiner des lignes verticales pour simuler des barres pour chaque genre
    for (i <- indices.toArray) {
      p += plot(Array(i, i), Array(0.0, quantities(i.toInt - 1)), '-', colorcode = "blue")
    }

    p.xlabel = "Genre"
    p.ylabel = "Nombre d'Employés"
    p.title = "Nombre d'Employés par Genre"

    f.refresh()
    f.saveas("employees_by_gender.png")

    //Sauvegarder le DataFrame en fichier Excel
    countByGender
      .write
      .format("com.crealytics.spark.excel")
      .option("header", "true") // Pour inclure la ligne d'entête dans le fichier Excel
      .option("dataAddress", "'Sheet1'!A1") // Spécifie l'onglet et la cellule de départ (optionnel)
      .mode("overwrite") // Pour écraser le fichier existant s'il existe
      .save(outputPath)

    spark.stop()
  }
}
