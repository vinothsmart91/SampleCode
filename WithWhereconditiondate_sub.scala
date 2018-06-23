import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

case class employee(eid: Int, doj: String)

object obje extends App {
  val conf = new SparkConf().setMaster("local[*]").setAppName("countnot3")
  val sc = new SparkContext(conf)
  val sqlcontext = new HiveContext(sc)
  val data = sc.textFile("C:\\Users\\611052474\\Downloads\\data\\data.txt")
  val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val splitRDD = data.map(x => x.split(",")).map(y => employee(y(0).toInt, y(1)))
  val result = sqlcontext.createDataFrame(splitRDD)

  import org.apache.spark.sql.functions._

  result.select("*").where(result("doj") < date_sub(current_date(), 40)).show()


}
