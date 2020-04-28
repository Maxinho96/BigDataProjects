package es2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class StockPricesJoinMapper extends Mapper<LongWritable,Text,Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] cols = StringUtils.getStrings(value.toString());

        String ticker = cols[0];
        String close_price = cols[2];
        String volume = cols[6];
        String date = cols[7];

        Text outputKey = new Text(ticker);

        String outputString = StringUtils.arrayToString(new String[] {"StockPrices", close_price, volume, date});
        Text outputValue = new Text(outputString);

        context.write(outputKey, outputValue);
    }
}
