package es3.primoJob;

import es3.customWritables.TextArrayWritable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

@Slf4j
public class PrimoMapperAziende extends Mapper<LongWritable, Text, Text, TextArrayWritable> {

    private static final String TAB_AZIENDE = "aziende";

    @SneakyThrows
    @Override
    public void map(LongWritable key, Text value, Context context) {

        String[] cols = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        //log.info(Arrays.toString(cols));

        //proseguo solamente se la lunghezza è 5
        if(cols.length != 5)
            return;

        String ticker = cols[0];

        String nomeAzienda = cols[2];
        cols = new String[]{TAB_AZIENDE, nomeAzienda}; //appartiene al file con i nomi delle aziende

        //log.info("SCRIVO TICKER: " + ticker + " - COLS: " + Arrays.toString(cols));
        context.write(new Text(ticker), new TextArrayWritable(cols));

    }

}