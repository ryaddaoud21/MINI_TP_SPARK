import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object CustomerRetentionRate {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("Customer Retention Rate")
      .master("local[*]")

      .getOrCreate()

    import spark.implicits._

    val ordersPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\data\\orders.csv"

    val ordersDF = spark.read.option("header", "true").option("inferSchema", "true").csv(ordersPath)

    // Compter le nombre de commandes par client
    val ordersPerCustomerDF = ordersDF
      .groupBy("CustomerID")
      .agg(countDistinct("SalesOrderID").alias("Orders_Count"))

    // Identifier les clients ayant commandé plus d'une fois
    val repeatCustomersCount = ordersPerCustomerDF
      .filter($"Orders_Count" > 1)
      .count()

    // Calculer le nombre total de clients distincts
    val totalCustomersCount = ordersDF
      .select("CustomerID")
      .distinct()
      .count()

    // Calculer le taux de fidélisation des clients
    val retentionRate = (repeatCustomersCount.toDouble / totalCustomersCount.toDouble) * 100

    println(s"Customer Retention Rate: $retentionRate%")

    // Créer un DataFrame pour le taux de rétention
    val retentionRateDF = Seq((retentionRate)).toDF("Customer Retention Rate")

    val outputPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\excel\\customer_retention_rate.xlsx"

    // Sauvegarder le DataFrame en fichier Excel
    retentionRateDF
      .write
      .format("com.crealytics.spark.excel")
      .option("header", "true") // Pour inclure la ligne d'entête
      .mode("overwrite") // Pour écraser le fichier existant s'il existe
      .save(outputPath)



    spark.stop()
  }
}
