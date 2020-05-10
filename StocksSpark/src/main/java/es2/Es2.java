package es2;

import org.apache.hadoop.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Es2 {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("es2.Es2");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> stocks = sc.textFile(args[0]);
        JavaRDD<String[]> splittedStocks = stocks.map(line -> line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));
        JavaPairRDD<String, String> mappedStocks = splittedStocks.mapToPair(cols -> new Tuple2<>(cols[0], cols[3]));

        JavaRDD<String> stockPrices = sc.textFile(args[1]);
        JavaRDD<String[]> splittedStockPrices = stockPrices.map(line -> line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));
        JavaPairRDD<String, String[]> mappedStockPrices = splittedStockPrices.mapToPair(cols -> new Tuple2<>(cols[0], new String[] {cols[2], cols[6], cols[7]}));

        JavaRDD<String[]> joined = mappedStocks
                .join(mappedStockPrices)
                .map(pair -> new String[]{pair._1(), pair._2()._1(), pair._2()._2()[0], pair._2()._2()[1], pair._2()._2()[2]});
        JavaRDD<String> joinedFormatted = joined.map(cols -> StringUtils.arrayToString(cols));

        joinedFormatted.saveAsTextFile(args[2]);

        //JavaPairRDD<String, Integer> ones = words.mapToPair(word -> new Tuple2<>(word, 1));
        //JavaPairRDD<String, Integer> counts = ones.reduceByKey((one1, one2) -> one1 + one2);

        // List<Tuple2<String, Integer>> output = counts.collect();

        // counts.saveAsTextFile(args[1]);

        sc.stop();
    }

}
