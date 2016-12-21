package sai.spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class WordCountDataFramesSQL {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);

		SQLContext sqlContext = new SQLContext(context);
		DataFrame df = sqlContext.jsonFile("/Users/user/Documents/PersonalPOCs/SparkProjects1/src/test/resources/test.json");
	    df.groupBy("name").count().show();
	   // df.select("name").show();
	    
	}

}
