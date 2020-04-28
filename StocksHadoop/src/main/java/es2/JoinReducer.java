package es2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Let's find the sector.
        String sector = null;
        for(Text value : values) {
            String[] cols = StringUtils.getStrings(value.toString());

            // This can be either StockPrices or Stocks, to identify from which table it comes from.
            String identifier = cols[0];

            if(identifier.equals("Stocks")) {
                sector = cols[1];
            }
        }

        // Don't do the join if we don't have the sector.
        if(sector != null) {
            // Let's do the join
            for(Text value : values) {
                String[] cols = StringUtils.getStrings(value.toString());

                // This can be either StockPrices or Stocks, to identify from which table it comes from.
                String identifier = cols[0];

                if(identifier.equals("StockPrices")) {
                    String close_price = cols[1];
                    String volume = cols[2];
                    String date = cols[3];

                    String outputString = StringUtils.arrayToString(new String[] {sector, close_price, volume, date});
                    Text outputValue = new Text(outputString);

                    context.write(key, outputValue);
                }
            }
        }
    }
}
