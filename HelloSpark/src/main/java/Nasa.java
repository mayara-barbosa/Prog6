import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Nasa {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf();
		conf.setAppName("Hello Spark");
		conf.setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);	
		
		List<String> hosts_nasa = new ArrayList<String>();
		
		JavaRDD<String> nasa_19950701 = sc.textFile("in/nasa_19950701.tsv");
		JavaRDD<String> nasa_19950801 = sc.textFile("in/nasa_19950801.tsv");
		
		JavaRDD<String> hostsNasa1RDD= nasa_19950701.map(host -> host.split("	")[0]);
		JavaRDD<String> hostsNasa2RDD= nasa_19950801.map(host -> host.split("	")[0]);
		JavaRDD<String> intersectionHosts = hostsNasa1RDD.intersection(hostsNasa2RDD);
		JavaRDD<String> result = intersectionHosts.filter(linha -> !linha.equals("host"));
	
		hosts_nasa = result.collect();
		
		for (String a : hosts_nasa) {
			System.out.println(a);
		}
		
		
		
		
	}
	
	

}
