package es2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.LinkedList;

public class JoinReducer extends Reducer<Text, Text, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // Let's find the sector.
        String sector = null;
        // We save in a list the visisted values that are not sector.
        LinkedList<String> visited = new LinkedList<String>();
        for(Text value : values) {
            String[] cols = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

            // This can be either StockPrices or Stocks, to identify from which table it comes from.
            String identifier = cols[0];

            // When we find the sector, we save it and stop iterating.
            if(identifier.equals("Stocks")) {
                sector = cols[1];
                break;
            }
            else {
                visited.add(value.toString());
            }
        }

        // Don't do the join if we don't have the sector.
        if(sector != null) {
            // Let's do the join with previous visited values and remaining values.
            joinWithSector(visited, context, sector);
            joinWithSector(values, context, sector);
        }
    }

    private void joinWithSector(Iterable<?> values, Context context, String sector) throws IOException, InterruptedException {
        for(Object value : values) {
            String[] cols = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

            String close_price = cols[1];
            String volume = cols[2];
            String date = cols[3];

            String outputString = StringUtils.arrayToString(new String[] {sector, close_price, volume, date});
            Text outputValue = new Text(outputString);

            context.write(NullWritable.get(), outputValue);
        }
    }
}
