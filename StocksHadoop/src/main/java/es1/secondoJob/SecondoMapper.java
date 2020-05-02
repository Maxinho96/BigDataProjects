package es1.secondoJob;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import es1.customExceptions.ParseTickerDataException;
import es1.customWritables.TickerDataWithoutDiffPercQuotWritable;
import es1.domainObjects.TickerData;

import java.util.StringTokenizer;

@Slf4j
public class SecondoMapper extends Mapper<LongWritable, Text, IntWritable, TickerDataWithoutDiffPercQuotWritable> {

    @SneakyThrows
    @Override
    protected void map(LongWritable key, Text value, Context context) {

        TickerData tickerData;

        try{
            tickerData = createTickerData(value);
        } catch(ParseTickerDataException e){
            return; //TODO FORSE: rilanciare eccezione anziche scartare e basta?
        }

        //costruisco l'oggetto da passare al reducer con tutti i campi del record letto tranne 'diffPercQuot' che sarà la key
        TickerDataWithoutDiffPercQuotWritable tickerDataWithoutDiffPercQuotWritable = new TickerDataWithoutDiffPercQuotWritable(
                tickerData.getTicker(),
                tickerData.getMinClose(),
                tickerData.getMaxClose(),
                tickerData.getAvgVolume()
        );

        //log.info("scrivo key: " + tickerData.getDiffPercQuot() + " value: " + tickerDataWithoutDiffPercQuotWritable.toString());
        context.write(new IntWritable(tickerData.getDiffPercQuotRoundedDown()), tickerDataWithoutDiffPercQuotWritable);
    }

    private TickerData createTickerData(Text line) throws ParseTickerDataException {

        String record = line.toString();
        StringTokenizer stringTokenizer = new StringTokenizer(record, ",");

        if(stringTokenizer.countTokens() != 5)
            throw new ParseTickerDataException("Scartata linea malformattata, non contiene esattamente 5 campi: " + record);

        String ticker = stringTokenizer.nextToken();

        //key
        int diffPercQuotRoundedDown;
        try {
            diffPercQuotRoundedDown = Integer.parseInt(stringTokenizer.nextToken());
        }catch (NumberFormatException e){
            throw new ParseTickerDataException("Scartata linea malformattata, campo 'diffPercQuotRoundedDown' malformattato: " + record);
        }

        float minClose;
        try {
            minClose = Float.parseFloat(stringTokenizer.nextToken());
        }catch (NumberFormatException e){
            throw new ParseTickerDataException("Scartata linea malformattata, campo 'minClose' malformattato: " + record);
        }

        float maxClose;
        try {
            maxClose = Float.parseFloat(stringTokenizer.nextToken());
        }catch (NumberFormatException e){
            throw new ParseTickerDataException("Scartata linea malformattata, campo 'maxClose' malformattato: " + record);
        }

        float avgVolume;
        try {
            avgVolume = Float.parseFloat(stringTokenizer.nextToken());
        }catch (NumberFormatException e){
            throw new ParseTickerDataException("Scartata linea malformattata, campo 'avgVolume' malformattato: " + record);
        }

        //creo e ritorno l'oggetto TickerDataWithoutDiffPercQuotWritable
        return new TickerData(ticker, diffPercQuotRoundedDown, minClose, maxClose, avgVolume);
    }
}
