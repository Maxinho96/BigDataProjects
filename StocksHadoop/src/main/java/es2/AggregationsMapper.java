package es2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class AggregationsMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] cols = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

        String ticker = cols[0];
        String sector = cols[1];
        String close_price = cols[2];
        String volume = cols[3];
        String date = cols[4];

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date dateParsed = null;
        try {
            dateParsed = dateFormat.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        int year = dateParsed.getYear() + 1900;

        if (year >= 2008 && year <= 2018) {
            String outputKeyString = StringUtils.arrayToString(new String[]{sector, Integer.toString(year)});
            Text outputKey = new Text(outputKeyString);

            String outputValueString = StringUtils.arrayToString(new String[]{ticker, close_price, volume, date});
            Text outputValue = new Text(outputValueString);

            context.write(outputKey, outputValue);
        }
    }

}
