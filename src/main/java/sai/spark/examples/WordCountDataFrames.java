package sai.spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class WordCountDataFrames {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);

		SQLContext sqlContext = new SQLContext(context);
		DataFrame df = sqlContext.jsonFile("/Users/user/Documents/PersonalPOCs/SparkProjects1/src/test/resources/test.json");
		df.registerTempTable("Users1");
		DataFrame dfTable2 = sqlContext.jsonFile("/Users/user/Documents/PersonalPOCs/SparkProjects1/src/test/resources/testTable.json");
		dfTable2.registerTempTable("Users2");
		DataFrame values = sqlContext.sql("SELECT us1.name as Name,count(*) as Total FROM Users1 us1 INNER JOIN Users2 us2 ON us1.name = us2.name group by us1.name");
		values.show();
		//df.join(dfTable2).show();

		// df.groupBy("name").count().show();
		// df.show(5);
		// DataFrame values = sqlContext.sql("SELECT name FROM cars");
		// values.show();
	}

}
