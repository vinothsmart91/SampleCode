import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SQLImplicits;
import org.apache.spark.sql.functions._;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types._;

object test extends App
{
  val conf=new SparkConf().setMaster("local[*]").setAppName("Checkingdata")
  val sc= new SparkContext(conf)
  val sQLContext= new SQLContext(sc)
  import sQLContext.implicits._
  val data= sc.parallelize(Seq(("BT",2000),("Harman",5000))).toDF("companyname","credit")
  val scheme=data.schema
  val dataschema= data.rdd.zipWithUniqueId.map{case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq)}
  val result=sQLContext.createDataFrame(dataschema,StructType(StructField("id", LongType, false) +: scheme.fields))
  result.coalesce(1).write.parquet("C:\\Users\\611052474\\Downloads\\output1")
}
