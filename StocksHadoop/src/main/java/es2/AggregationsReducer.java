package es2;

import javafx.util.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class AggregationsReducer extends Reducer<Text, Text, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Devo fare:
        // - Per ogni ticker, calcolare la somma dei volumi, e poi fare la media di tutti questi valori.
        // - Per ogni ticker, calcolare la differenza percentuale tra ultimo close_price utile e primo close_price
        //   utile, e poi fare la media di tutti questi valori.
        // - Media di close_price.

        // Qua salvo via via la somma dei volumi per ogni ticker.
        Map<String, Long> tickerToVolumeSum = new HashMap<String, Long>();
        // Qua salvo via via la prima data utile e l'ultima data utile per ogni ticker
        Map<String, Pair<Date, Date>> tickerToDates = new HashMap<String, Pair<Date, Date>>();
        // Qua salvo via via il primo close_price utile e l'ultimo close_price utile per ogni ticker
        Map<String, Pair<Float, Float>> tickerToClosePrices = new HashMap<String, Pair<Float, Float>>();
        // Qua salvo via via la somma e il conteggio dei close_price.
        float closePricesSum = 0;
        int numClosePrices = 0;

        for(Text value : values) {
            String[] cols = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

            String ticker = cols[0];
            float close_price = Float.parseFloat(cols[1]);
            long volume = Long.parseLong(cols[2]);
            String date = cols[3];

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date parsedDate = null;
            try {
                parsedDate = dateFormat.parse(date);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            // Aggiorno la somma del volume per il ticker corrente.
            long volumeSum = tickerToVolumeSum.containsKey(ticker) ? tickerToVolumeSum.get(ticker) : 0;
            tickerToVolumeSum.put(ticker, volumeSum + volume);

            // Aggiorno prima e ultima data utile e primo e ultimo close_price utile per il ticker corrente.
            if(!tickerToDates.containsKey(ticker)) {
                tickerToDates.put(ticker, new Pair<Date, Date>(parsedDate, parsedDate));
                tickerToClosePrices.put(ticker, new Pair<Float, Float>(close_price, close_price));
            }
            else {
                Date oldLeftDate = tickerToDates.get(ticker).getKey();
                Date oldRightDate = tickerToDates.get(ticker).getValue();
                float oldLeftClosePrice = tickerToClosePrices.get(ticker).getKey();
                float oldRightClosePrice = tickerToClosePrices.get(ticker).getValue();

                if(parsedDate.before(oldLeftDate)) {
                    tickerToDates.put(ticker, new Pair<Date, Date>(parsedDate, oldRightDate));
                    tickerToClosePrices.put(ticker, new Pair<Float, Float>(close_price, oldRightClosePrice));
                }
                else if(parsedDate.after(oldRightDate)) {
                    tickerToDates.put(ticker, new Pair<Date, Date>(oldLeftDate, parsedDate));
                    tickerToClosePrices.put(ticker, new Pair<Float, Float>(oldLeftClosePrice, close_price));
                }
            }

            // Aggiorno la somma e il conteggio dei prezzi di chiusura.
            closePricesSum += close_price;
            numClosePrices++;

        }

        // Calcolo la media di tutte le somme di volume di ogni ticker.
        float allVolumesSum = 0;
        for(long singleVolumeSum : tickerToVolumeSum.values()) {
            allVolumesSum += singleVolumeSum;
        }
        float volumeAvg = allVolumesSum / tickerToVolumeSum.values().size();
        String volumeOutput = String.format(Locale.ROOT, "%.0f", volumeAvg);

        // Calcolo per ogni ticker la differenza percentuale tra primo e ultimo close_price utili, e poi
        // la media di tutti questi valori.
        float allVariationsSum = 0;
        for(Pair<Float, Float> singleClosePrices : tickerToClosePrices.values()) {
            float singleLeftClosePrice = singleClosePrices.getKey();
            float singleRightClosePrice = singleClosePrices.getValue();

            float singleVariation = (singleRightClosePrice - singleLeftClosePrice) / singleLeftClosePrice;

            allVariationsSum += singleVariation;
        }
        float variationAvg = allVariationsSum / tickerToClosePrices.values().size();
        String variationOutput = String.format(Locale.ROOT, "%.2f", variationAvg);

        // Calcolo la media dei prezzi di chiusura.
        float closePricesAvg = closePricesSum / numClosePrices;
        String closePricesOutput = String.format(Locale.ROOT, "%.2f", closePricesAvg);

        String outputString = StringUtils.arrayToString(new String[] {key.toString(), volumeOutput, variationOutput, closePricesOutput});
        Text outputValue = new Text(outputString);

        context.write(NullWritable.get(), outputValue);
    }

}
