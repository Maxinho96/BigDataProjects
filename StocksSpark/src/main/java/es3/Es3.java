package es3;

import org.apache.hadoop.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.*;

import java.lang.Float;
import java.lang.Long;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class Es3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("es2.Es2");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> stocks = sc.textFile(args[0]);
        JavaRDD<String[]> splittedStocks = stocks.map(line -> line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));
        JavaPairRDD<String, String> mappedStocks = splittedStocks
                .filter(cols -> !cols[0].equals("ticker"))
                .mapToPair(cols -> new Tuple2<>(cols[0], cols[2]));

        JavaRDD<String> stockPrices = sc.textFile(args[1]);
        JavaRDD<String[]> splittedStockPrices = stockPrices
                .map(line -> line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))
                .filter(cols -> {
                    if(cols[0].equals("ticker")) {
                        return false;
                    }
                    int year = new SimpleDateFormat("yyyy-MM-dd").parse(cols[7]).getYear() + 1900;
                    return year >= 2016 && year <= 2018;
                });
        JavaPairRDD<String, String[]> mappedStockPrices = splittedStockPrices.mapToPair(cols -> new Tuple2<>(cols[0], new String[] {cols[2], cols[7]}));

        JavaRDD<String[]> joined = mappedStocks
                .join(mappedStockPrices)
                .map(pair -> new String[] {pair._1(), pair._2()._1(), pair._2()._2()[0], pair._2()._2()[1]});

        JavaPairRDD<Tuple3<String, Integer, String>, Tuple4<Date, Date, Float, Float>> mappedByNameYearTicker = joined
                .mapToPair(cols -> {
                    String ticker = cols[0];
                    String name = cols[1];
                    Float closePrice = Float.parseFloat(cols[2]);
                    Date date = new SimpleDateFormat("yyyy-MM-dd").parse(cols[3]);
                    Integer year = date.getYear() + 1900;

                    Tuple3<String, Integer, String> key = new Tuple3<>(name, year, ticker);
                    Tuple4<Date, Date, Float, Float> value = new Tuple4<>(date, date, closePrice, closePrice);

                    return new Tuple2<>(key, value);
                });
        JavaPairRDD<Tuple3<String, Integer, String>, Tuple4<Date, Date, Float, Float>> reducedByNameYearTicker = mappedByNameYearTicker
                .reduceByKey((leftTuple, rightTuple) -> {
                    Date leftFirstDate = leftTuple._1();
                    Date leftLastDate = leftTuple._2();
                    Float leftFirstClosePrice = leftTuple._3();
                    Float leftLastClosePrice = leftTuple._4();

                    Date rightFirstDate = rightTuple._1();
                    Date rightLastDate = rightTuple._2();
                    Float rightFirstClosePrice = rightTuple._3();
                    Float rightLastClosePrice = rightTuple._4();

                    Date newFirstDate = leftFirstDate;
                    Date newLastDate = leftLastDate;
                    Float newFirstClosePrice = leftFirstClosePrice;
                    Float newLastClosePrice = leftLastClosePrice;
                    if(rightFirstDate.before(leftFirstDate)) {
                        newFirstDate = rightFirstDate;
                        newFirstClosePrice = rightFirstClosePrice;
                    }
                    else if(rightLastDate.after(leftLastDate)) {
                        newLastDate = rightLastDate;
                        newLastClosePrice = rightLastClosePrice;
                    }

                    return new Tuple4<>(newFirstDate, newLastDate, newFirstClosePrice, newLastClosePrice);
                });

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Float, Integer>> mappedByNameYear = reducedByNameYearTicker
                .mapToPair(pair -> {
                    Tuple3<String, Integer, String> oldKey = pair._1();
                    Tuple2<String, Integer> newKey = new Tuple2<>(oldKey._1(), oldKey._2());

                    Tuple4<Date, Date, Float, Float> oldValue = pair._2();
                    Float oldFirstClosePrice = oldValue._3();
                    Float oldLastClosePrice = oldValue._4();
                    Float newVariation = ((oldLastClosePrice - oldFirstClosePrice) / oldFirstClosePrice) * 100;
                    Tuple2<Float, Integer> newValue = new Tuple2<>(newVariation, 1);

                    return new Tuple2<>(newKey, newValue);
                });
        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Float, Integer>> reducedByNameYear = mappedByNameYear
                .reduceByKey((leftTuple, rightTuple) -> {
                    Float leftVariation = leftTuple._1();
                    Integer leftTickerCount = leftTuple._2();

                    Float rightVariation = rightTuple._1();
                    Integer rightTickerCount = rightTuple._2();

                    Float newVariationsSum = leftVariation + rightVariation;

                    Integer newTickerCount = leftTickerCount + rightTickerCount;

                    return new Tuple2<Float, Integer>(newVariationsSum, newTickerCount);
                });

//        JavaRDD<String> result = reducedByNameYear
//                .map(pair ->  {
//                    String name = pair._1()._1();
//                    Integer year = pair._1()._2();
//
//                    Float variationsSum = pair._2()._1();
//                    Integer tickerCount = pair._2()._2();
//
//                    Float variationAvg = variationsSum / tickerCount;
//                    String variationOutput = String.format(Locale.ROOT, "%.2f", variationAvg);
//
//                    String[] resultArray = new String[] {name, year.toString(), variationOutput};
//
//                    return StringUtils.arrayToString(resultArray);
//                });
//
//        result.saveAsTextFile(args[2]);

        JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> mappedByName = reducedByNameYear
                .mapToPair(pair -> {
                    String name = pair._1()._1();
                    Integer year = pair._1()._2();

                    Float variationsSum = pair._2()._1();
                    Integer tickerCount = pair._2()._2();

                    Integer variationAvg = (int) (variationsSum / tickerCount);

                    Tuple3<Integer, Integer, Integer> value = null;
                    switch(year) {
                        case 2016:
                            value = new Tuple3<>(variationAvg, null, null);
                            break;
                        case 2017:
                            value = new Tuple3<>(null, variationAvg, null);
                            break;
                        case 2018:
                            value = new Tuple3<>(null, null, variationAvg);
                    }

                    return new Tuple2<>(name, value);
                });
        JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> reducedByName = mappedByName
                .reduceByKey((leftTuple, rightTuple) -> {
                    Integer leftFirstVariation = leftTuple._1();
                    Integer leftSecondVariation = leftTuple._2();
                    Integer leftThirdVariation = leftTuple._3();

                    Integer rightFirstVariation = rightTuple._1();
                    Integer rightSecondVariation = rightTuple._2();
                    Integer rightThirdVariation = rightTuple._3();

                    Integer newFirstVariation = valueOrNull(leftFirstVariation, rightFirstVariation);
                    Integer newSecondVariation = valueOrNull(leftSecondVariation, rightSecondVariation);
                    Integer newThirdVariation = valueOrNull(leftThirdVariation, rightThirdVariation);

                    return new Tuple3<>(newFirstVariation, newSecondVariation, newThirdVariation);
                })
                .filter(pair -> {
                    Integer firstVariation = pair._2()._1();
                    Integer secondVariation = pair._2()._2();
                    Integer thirdVariation = pair._2()._3();

                    return (firstVariation != null) && (secondVariation != null) && (thirdVariation != null);
                });

        reducedByName.saveAsTextFile(args[2]);
    }

    private static Integer valueOrNull(Integer value1, Integer value2) {
        if(value1 != null) {
            return value1;
        }
        return value2;
    }
}
