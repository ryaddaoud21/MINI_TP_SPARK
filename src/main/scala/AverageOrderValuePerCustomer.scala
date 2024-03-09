import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import breeze.plot._
import breeze.linalg._

object AverageOrderValuePerCustomer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Average Order Value Per Customer with FullName")
      .master("local[*]")
      .getOrCreate()

    // Chemins vers les fichiers
    val ordersPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\data\\orders.csv"
    val customersPath = "C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\data\\customers.csv"

    // Charger les DataFrames
    val ordersDF = spark.read.option("header", "true").option("inferSchema", "true").csv(ordersPath)
    val customersDF = spark.read.option("header", "true").option("inferSchema", "true").csv(customersPath)

    // Calcul de la somme des totaux de commande et du nombre de commandes par client
    val totalAndCountByCustomerDF = ordersDF
      .groupBy("CustomerID")
      .agg(
        org.apache.spark.sql.functions.sum("TotalDue").alias("Total_Sales"),
        count("SalesOrderID").alias("Number_of_Orders")
      )

    // Calcul de la valeur moyenne des commandes par client
    val averageOrderValuePerCustomerDF = totalAndCountByCustomerDF
      .withColumn("Average_Order_Value", col("Total_Sales") / col("Number_of_Orders"))

    // Joindre avec les informations sur les clients pour inclure FullName
    val averageOrderValueWithCustomerNameDF = averageOrderValuePerCustomerDF
      .join(customersDF, "CustomerID")
      .select("CustomerID","FullName", "Average_Order_Value")
      .orderBy(desc("Average_Order_Value"))

    // Afficher les résultats
    averageOrderValueWithCustomerNameDF.show()

    val data = averageOrderValueWithCustomerNameDF.collect()
    val averageOrderValues = data.map(_.getAs[Double]("Average_Order_Value"))

    val f = Figure()
    val p = f.subplot(0)
    val x = DenseVector.rangeD(1, data.length + 1, 1)
    val y = DenseVector(averageOrderValues)

    p += plot(x, y, '-', colorcode = "blue")
    p += plot(x, y, '+', colorcode = "red")

    p.xlabel = "Customers (Indexed)"
    p.ylabel = "Average Order Value"
    p.title = "Average Order Value Per Customer"

    f.refresh()

    // Sauvegarde du graphique
    f.saveas("C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\save\\average_order_value_per_customer.png")

    averageOrderValueWithCustomerNameDF
      .write
      .format("com.crealytics.spark.excel")
      .option("header", "true") // Pour inclure la ligne d'entête dans le fichier Excel
      .option("dataAddress", "'Sheet1'!A1") // Nom de la feuille et cellule de départ
      .mode("overwrite") // Pour écraser un fichier existant
      .save("C:\\Users\\HP\\IdeaProjects\\MiniProjet_SPARK\\excel\\average_order_value_per_customer.xlsx")

    spark.stop()
  }
}
