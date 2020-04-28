package es2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class StocksJoinMapper extends Mapper<LongWritable, Text,Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] cols = StringUtils.getStrings(value.toString());

        String ticker = cols[0];
        String sector = cols[3];

        Text outputKey = new Text(ticker);

        String outputString = StringUtils.arrayToString(new String[]{"Stocks", sector});
        Text outputValue = new Text(outputString);

        context.write(outputKey, outputValue);
    }
}
