package es1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple8;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringJoiner;

public class Es1 {

    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("es1.Es1");
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> stockPrices = sc.textFile(args[0]);

        JavaRDD<String[]> splittedStockPrices = stockPrices
                .map(line -> line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))
                .filter(cols -> {
                    if (cols[0].equals("ticker") || cols.length != 8) {
                        return false;
                    }
                    try{
                        Float.parseFloat(cols[2]); //2=close
                        Float.parseFloat(cols[6]); //6=volume
                        simpleDateFormat.parse(cols[7]); //7=date
                    }catch (NumberFormatException | ParseException e){
                        return false; //campi malformattati
                    }
                    int year = simpleDateFormat.parse(cols[7]).getYear() + 1900;
                    return year >= 2008 && year <= 2018;
                });

        JavaPairRDD<String, Tuple8<Date,Date,Float,Float,Float,Float,Float,Integer>> mappedStockPrices = splittedStockPrices.mapToPair(cols -> {

            float close = Float.parseFloat(cols[2]); //2=close
            float volume = Float.parseFloat(cols[6]); //6=volume
            Date date = simpleDateFormat.parse(cols[7]); //7=date

            //key=ticker, creo la tupla 1=minDate,2=maxDate,3=closeOfMinDate,4=closeOfMaxDate,5=minClose,6=maxClose,7=sumVolume,8=totVolume
            return new Tuple2<>(cols[0], new Tuple8<>(date, date, close, close, close, close, volume, 1));
        });

        JavaPairRDD<String, Tuple8<Date,Date,Float,Float,Float,Float,Float,Integer>> reducedStockPrices = mappedStockPrices.reduceByKey( (leftTuple, rightTuple) -> {

            Date minDate;
            Date maxDate;

            Float closeOfMinDate;
            Float closeOfMaxDate;

            Float minClose = Math.min(leftTuple._5(), rightTuple._5());
            Float maxClose = Math.max(leftTuple._6(), rightTuple._6());

            Float sumVolume = leftTuple._7() + rightTuple._7();
            int totVolume = leftTuple._8() + rightTuple._8();

            if(leftTuple._1().compareTo(rightTuple._1()) <= 0){
                minDate = leftTuple._1();
                closeOfMinDate = leftTuple._3();
            }
            else{
                minDate = rightTuple._1();
                closeOfMinDate = rightTuple._3();
            }

            if(leftTuple._2().compareTo(rightTuple._2()) >= 0){
                maxDate = leftTuple._2();
                closeOfMaxDate = leftTuple._4();
            }
            else{
                maxDate = rightTuple._2();
                closeOfMaxDate = rightTuple._4();
            }

            //creo la tupla 1=minDate,2=maxDate,3=closeOfMinDate,4=closeOfMaxDate,5=minClose,6=maxClose,7=sumVolume,8=totVolume
            return new Tuple8<>(minDate,maxDate,closeOfMinDate,closeOfMaxDate,minClose,maxClose,sumVolume,totVolume);

        });


        JavaPairRDD<Integer, Tuple4<String, Float, Float, Float>> sortedDataWithVarPercRoundedDownAsKey = reducedStockPrices.mapToPair(tuple -> {

            int varPercRoundedDown = (int) ( ( ( tuple._2()._4() - tuple._2()._3() ) / tuple._2()._3() ) * 100);
            float avgVolume = tuple._2()._7() / tuple._2()._8();

            //key=varPercRoundedDown, creo la tupla 1=ticker,2=prezzo minimo, 3=prezzo massimo, 4=volume medio
            return new Tuple2<>(varPercRoundedDown, new Tuple4<>(tuple._1(), tuple._2()._5(), tuple._2()._6(), avgVolume));
        }).sortByKey(false); //sort discendente


        JavaRDD<String> result = sortedDataWithVarPercRoundedDownAsKey.map( tuple ->{

            StringJoiner sj = new StringJoiner(",");

            sj.add(tuple._2()._1()); //ticker
            sj.add(String.valueOf(tuple._1())); //variazione percentuale arrotondata per difetto
            sj.add(String.valueOf(tuple._2()._2())); //prezzo minimo
            sj.add(String.valueOf(tuple._2()._3())); //prezzo massimo
            sj.add(String.valueOf(tuple._2()._4())); //volume medio

            return sj.toString();
        });


        result.saveAsTextFile(args[1]);
    }
}
