package es1.secondoJob;

import es1.customWritables.TextArrayWritable;
import es1.customWritables.TickerDataWithoutDiffPercQuotWritable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

@Slf4j
public class SecondoReducer extends Reducer<IntWritable, TickerDataWithoutDiffPercQuotWritable, Text, TextArrayWritable> {

    @SneakyThrows
    @Override
    public void reduce(IntWritable key, Iterable<TickerDataWithoutDiffPercQuotWritable> values, Context context) {

        for (TickerDataWithoutDiffPercQuotWritable value: values) {

            String[] result = new String[4];
            //il primo campo in output sarà 'diffPercQuot' ovvero la key prodotta dal mapper
            result[0] = key.toString();
            result[1] = String.valueOf(value.getMinClose());
            result[2] = String.valueOf(value.getMaxClose());
            result[3] = String.valueOf(value.getAvgVolume());

            context.write(new Text(value.getTicker()), new TextArrayWritable(result) );
        }
    }
}
