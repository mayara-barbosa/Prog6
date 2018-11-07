import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SomaPrimos {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf();
		conf.setAppName("Hello Spark");
		conf.setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
				JavaRDD<String> primos = sc.textFile("in/prime_nums.text");
		
		JavaRDD<String> linhas = primos.flatMap(linha -> Arrays.asList(linha.split("\t")).iterator());
		JavaRDD<Integer> intPrimos = linhas.map(l -> Integer.parseInt(l.trim()));//trim tira os espaços da frente e de tras;
		long r = intPrimos.reduce((x,y) -> x+y);
		
		
		System.out.println(r);
		
		
	}

}
