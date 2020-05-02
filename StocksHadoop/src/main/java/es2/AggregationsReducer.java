package es2;

//MARCO WATCHES YOU//import javafx.util.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
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
        // Qua salvo via via il primo close_price utile e l'ultimo close_price utile per ogni ticker
        //MARCO WATCHES YOU//Map<String, Pair<Float, Float>> tickerToClosePrices = new HashMap<String, Pair<Float, Float>>();
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
            Date dateParsed = null;
            try {
                dateParsed = dateFormat.parse(date);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            // Aggiorno la somma del volume per il ticker corrente.
            long volumeSum = tickerToVolumeSum.containsKey(ticker) ? tickerToVolumeSum.get(ticker) : 0;
            tickerToVolumeSum.put(ticker, volumeSum + volume);

            // TODO: aggiornare primo e ultimo close_price utile per il ticker corrente.

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
        String volumeOutput = Float.toString(volumeAvg);

        // TODO: calcolare per ogni ticker la differenza percentuale tra primo e ultimo close_price utili, e poi
        // la media di tutti questi valori.
        String variationAvg = "";

        // Calcolo la media dei prezzi di chiusura.
        float closePricesAvg = closePricesSum / numClosePrices;
        String closePricesOutput = Float.toString(closePricesAvg);

        String outputString = StringUtils.arrayToString(new String[] {key.toString(), volumeOutput, variationAvg, closePricesOutput});
        Text outputValue = new Text(outputString);

        context.write(NullWritable.get(), outputValue);
    }

}
