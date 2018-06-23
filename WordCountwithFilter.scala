import org.apache.spark.{SparkConf, SparkContext}

object obje extends App {
  val conf = new SparkConf().setMaster("local[*]").setAppName("countnot3")
  val sc = new SparkContext(conf)
  val data = sc.textFile("C:\\Users\\611052474\\Downloads\\data\\data.txt")
  val splitRDD = data.flatMap(x => x.split(",")).filter(x => x.length != 3).map(x => (x, 1))
  val resultRDD = splitRDD.reduceByKey((x, y) => x + y)
  resultRDD.foreach(println)

}
