import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;


public class Aeroporto {
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf();
		conf.setAppName("Hello Spark");
		conf.setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);		
		
		List<String> aeroportos = new ArrayList<String>();
		JavaRDD<String> aeroportosRDD = sc.textFile("in/airports.text");
		
		JavaRDD<String> linhasFiltradas = aeroportosRDD.filter(linha -> linha.split(",")[3].equals("\"Brazil\"")//retorna conjuntos
		);
		JavaRDD<String> aeroportosBRMaiusculo = linhasFiltradas.map(linha -> linha.toUpperCase());
		JavaRDD<Integer> aeroQntChar = aeroportosBRMaiusculo.map(linha -> linha.length());//qnt de caracteres na linha
		
		JavaRDD<String> nomecidade= linhasFiltradas.map(aero -> aero.split(",")[1]+" " + aero.split(",")[2] );
	    JavaRDD<String> latitude = linhasFiltradas.filter(f -> 
	    										Integer.parseInt(f.split(",")[6]) >= 40);
		
		long quantidade = linhasFiltradas.count();
		aeroportos = aeroportosBRMaiusculo.collect();
				
		for (String a : aeroportos) {
			System.out.println(a);
		}
		//aeroportosRDD.saveAsTextFile("out/aeroportos");
	}
}
