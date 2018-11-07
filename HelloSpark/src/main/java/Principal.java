import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;


public class Principal {
	//public static void main(String[] args) {
	public void teste() {
		// TODO Auto-generated method stub

		org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf();
		conf.setAppName("Hello Spark");
		conf.setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> lista = Arrays.asList(1, 2, 3, 4, 5);	
		JavaRDD<Integer> listaRDD = sc.parallelize(lista);
		
		JavaRDD<Integer> listaQuadrad = listaRDD.map(n -> (n* n));//MAP transformacao
		
		JavaRDD<String> linhas = sc.textFile("in/word_count.text");
		//JavaRDD<String> palavras = linhas.flatMap(linha -> linha.split(" "));
		
		JavaRDD<String> linhasFiltradas = linhas.filter(linha -> linha.contains("New York"));
		
		//Receber somente os números pares da lista de números inteiros.
		JavaRDD<Integer> listaParesRDD = listaRDD.filter(numero -> (numero%2 == 0));
		
		//Operação -> Sai do distribuido e retorna para o mestre
		
		long quantidade = listaParesRDD.count();
		System.out.println("Números pares: "+ quantidade);
		quantidade = linhasFiltradas.count();
		lista = listaParesRDD.collect();
		
		for (int v : lista) {
			System.out.println("Valor: "+ v);
		}
		
		System.out.println("Linhas filtradas: "+ quantidade);
		
		List<String> transacoes = new ArrayList<String>();
		
		JavaRDD<String> transacoesRDD = sc.textFile("in/Transacoes.csv");
		JavaRDD<String> amostraRDD = transacoesRDD.sample(true, 0.3);
		transacoesRDD.saveAsTextFile("out/transacoes");
		
		transacoes = amostraRDD.collect();
				
		for (String a : transacoes) {
			System.out.println(a);
		}
		
		
	}
}
