package es2;

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

public class Es2 {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("es2.Es2");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> stocks = sc.textFile(args[0]);
        JavaRDD<String[]> splittedStocks = stocks.map(line -> line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));
        JavaPairRDD<String, String> mappedStocks = splittedStocks
                .filter(cols -> !cols[0].equals("ticker"))
                .mapToPair(cols -> new Tuple2<>(cols[0], cols[3]));

        JavaRDD<String> stockPrices = sc.textFile(args[1]);
        JavaRDD<String[]> splittedStockPrices = stockPrices
                .map(line -> line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))
                .filter(cols -> {
                            if(cols[0].equals("ticker")) {
                                return false;
                            }
                            int year = new SimpleDateFormat("yyyy-MM-dd").parse(cols[7]).getYear() + 1900;
                            return year >= 2008 && year <= 2018;
                        });
        JavaPairRDD<String, String[]> mappedStockPrices = splittedStockPrices.mapToPair(cols -> new Tuple2<>(cols[0], new String[] {cols[2], cols[6], cols[7]}));

        JavaRDD<String[]> joined = mappedStocks
                .join(mappedStockPrices)
                .map(pair -> new String[] {pair._1(), pair._2()._1(), pair._2()._2()[0], pair._2()._2()[1], pair._2()._2()[2]});

        // JavaRDD<String> joinedFormatted = joined.map(cols -> StringUtils.arrayToString(cols));
        // joinedFormatted.saveAsTextFile(args[2]);

        JavaPairRDD<Tuple3<String, Integer, String>, Tuple7<Float, Long, Date, Date, Float, Float, Integer>> mappedBySectorYearTicker = joined
                .mapToPair(cols -> {
                    String ticker = cols[0];
                    String sector = cols[1];
                    Float closePrice = Float.parseFloat(cols[2]);
                    Long volume = Long.parseLong(cols[3]);
                    Date date = new SimpleDateFormat("yyyy-MM-dd").parse(cols[4]);
                    Integer year = date.getYear() + 1900;

                    Tuple3<String, Integer, String> key = new Tuple3<>(sector, year, ticker);
                    Tuple7<Float, Long, Date, Date, Float, Float, Integer> value = new Tuple7<>(closePrice, volume, date, date, closePrice, closePrice, 1);

                    return new Tuple2<>(key, value);
                });
        JavaPairRDD<Tuple3<String, Integer, String>, Tuple7<Float, Long, Date, Date, Float, Float, Integer>> reducedBySectorYearTicker = mappedBySectorYearTicker
                .reduceByKey((leftTuple, rightTuple) -> {
                    Float leftClosePricesSum = leftTuple._1();
                    Long leftVolumesSum = leftTuple._2();
                    Date leftFirstDate = leftTuple._3();
                    Date leftLastDate = leftTuple._4();
                    Float leftFirstClosePrice = leftTuple._5();
                    Float leftLastClosePrice = leftTuple._6();
                    Integer leftCount = leftTuple._7();

                    Float rightClosePricesSum = rightTuple._1();
                    Long rightVolumesSum = rightTuple._2();
                    Date rightFirstDate = rightTuple._3();
                    Date rightLastDate = rightTuple._4();
                    Float rightFirstClosePrice = rightTuple._5();
                    Float rightLastClosePrice = rightTuple._6();
                    Integer rightCount = rightTuple._7();

                    Float newClosePricesSum = leftClosePricesSum + rightClosePricesSum;

                    Long newVolumesSum = leftVolumesSum + rightVolumesSum;

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

                    Integer newCount = leftCount + rightCount;

                    return new Tuple7<>(newClosePricesSum, newVolumesSum, newFirstDate, newLastDate, newFirstClosePrice, newLastClosePrice, newCount);
                });

        // reducedBySectorYearTicker.saveAsTextFile(args[2]);

        JavaPairRDD<Tuple2<String, Integer>, Tuple5<Float, Long, Float, Integer, Integer>> mappedBySectorYear = reducedBySectorYearTicker
                .mapToPair(pair -> {
                    Tuple3<String, Integer, String> oldKey = pair._1();
                    Tuple2<String, Integer> newKey = new Tuple2<>(oldKey._1(), oldKey._2());

                    Tuple7<Float, Long, Date, Date, Float, Float, Integer> oldValue = pair._2();
                    Float oldClosePricesSum = oldValue._1();
                    Long oldVolumesSum = oldValue._2();
                    Float oldFirstClosePrice = oldValue._5();
                    Float oldLastClosePrice = oldValue._6();
                    Float newVariation = ((oldLastClosePrice - oldFirstClosePrice) / oldFirstClosePrice) * 100;
                    Integer oldCount = oldValue._7();
                    Tuple5<Float, Long, Float, Integer, Integer> newValue = new Tuple5<>(oldClosePricesSum, oldVolumesSum, newVariation, oldCount, 1);

                    return new Tuple2<>(newKey, newValue);
                });
        JavaPairRDD<Tuple2<String, Integer>, Tuple5<Float, Long, Float, Integer, Integer>> reducedBySectorYear = mappedBySectorYear
                .reduceByKey((leftTuple, rightTuple) -> {
                    Float leftClosePricesSum = leftTuple._1();
                    Long leftVolumesSum = leftTuple._2();
                    Float leftVariation = leftTuple._3();
                    Integer leftCount = leftTuple._4();
                    Integer leftTickerCount = leftTuple._5();

                    Float rightClosePricesSum = rightTuple._1();
                    Long rightVolumesSum = rightTuple._2();
                    Float rightVariation = rightTuple._3();
                    Integer rightCount = rightTuple._4();
                    Integer rightTickerCount = rightTuple._5();

                    Float newClosePricesSum = leftClosePricesSum + rightClosePricesSum;

                    Long newVolumesSum = leftVolumesSum + rightVolumesSum;

                    Float newVariationsSum = leftVariation + rightVariation;

                    Integer newCount = leftCount + rightCount;

                    Integer newTickerCount = leftTickerCount + rightTickerCount;

                    return new Tuple5<Float, Long, Float, Integer, Integer>(newClosePricesSum, newVolumesSum, newVariationsSum, newCount, newTickerCount);
                });

        JavaRDD<String> result = reducedBySectorYear
                .map(pair ->  {
                    String sector = pair._1()._1();
                    Integer year = pair._1()._2();

                    Float closePricesSum = pair._2()._1();
                    Long volumesSum = pair._2()._2();
                    Float variationsSum = pair._2()._3();
                    Integer count = pair._2()._4();
                    Integer tickerCount = pair._2()._5();

                    Float volumeAvg = volumesSum.floatValue() / tickerCount;
                    String volumeOutput = String.format(Locale.ROOT, "%.0f", volumeAvg);
                    Float variationAvg = variationsSum / tickerCount;
                    String variationOutput = String.format(Locale.ROOT, "%.2f", variationAvg);
                    Float closePricesAvg = closePricesSum / count;
                    String closePricesOutput = String.format(Locale.ROOT, "%.2f", closePricesAvg);

                    String[] resultArray = new String[] {sector, year.toString(), volumeOutput, variationOutput, closePricesOutput};

                    return StringUtils.arrayToString(resultArray);
                });

        result.saveAsTextFile(args[2]);

        sc.stop();
    }

}
