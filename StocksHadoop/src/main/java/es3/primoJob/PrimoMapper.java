package es3.primoJob;

import es3.customExceptions.ParseStockPricesRecordException;
import es3.customWritables.SelectedFieldsOfStockPricesRecordWritable;
import es3.domainObjects.StockPricesRecord;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

@Slf4j
public class PrimoMapper extends Mapper<LongWritable, Text, Text, SelectedFieldsOfStockPricesRecordWritable> {

    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private Date minDate = simpleDateFormat.parse("2016-01-01");
    private Date maxDate = simpleDateFormat.parse("2018-12-31");

    public PrimoMapper() throws ParseException {}

    @SneakyThrows
    @Override
    public void map(LongWritable key, Text value, Context context) {

        StockPricesRecord stockPricesRecord;

        try {
            stockPricesRecord = createStockPricesRecord(value);
        } catch (ParseStockPricesRecordException e) {
            //log.info(e.toString());
            return;
        }

        //sono interessato solamente ai record con anno >=2016 e <=2018
        if(stockPricesRecord.getDate().compareTo(this.minDate) < 0 || stockPricesRecord.getDate().compareTo(this.maxDate) > 0) {
            //log.info("Scartata linea con anno non compreso tra 2016 e 2018: " + value.toString());
            return;
        }

        //costruisco l'oggetto da passare al reducer con i campi utili del record letto
        SelectedFieldsOfStockPricesRecordWritable selectedFieldsOfStockPricesRecordWritable = new SelectedFieldsOfStockPricesRecordWritable(
                stockPricesRecord.getClose(),
                this.simpleDateFormat.format(stockPricesRecord.getDate())
        );

        //log.info("scrivo la coppia: " + stockPricesRecord.getTicker() + " - " + selectedFieldsOfStockPricesRecordWritable.toString());
        context.write(new Text(stockPricesRecord.getTicker()), selectedFieldsOfStockPricesRecordWritable);
    }

    /***
     * @param line la linea da processare e con cui creare l'oggetto StockPricesRecord
     * @return obj StockPricesRecord
     * @throws ParseStockPricesRecordException in caso la linea con i suoi campi non sia formattata correttamente
     */
    private StockPricesRecord createStockPricesRecord(Text line) throws ParseStockPricesRecordException{

        String record = line.toString();
        StringTokenizer stringTokenizer = new StringTokenizer(record, ",");

        if(stringTokenizer.countTokens() != 8)
            throw new ParseStockPricesRecordException("Scartata linea malformattata, non contiene esattamente 8 campi: " + record);

        //prelevo il primo campo (ticker)
        String ticker = stringTokenizer.nextToken();

        //verifico la correttezza del primo campo
        if(!ticker.matches("^[A-Za-z0-9]*$"))
            throw new ParseStockPricesRecordException("Scartata linea malformattata, campo 'ticker' malformattato: " + record);

        //prelevo il secondo campo (open) e ne verifico la correttezza
        float open;
        try {
            open = Float.parseFloat(stringTokenizer.nextToken());
        }catch (NumberFormatException e){
            throw new ParseStockPricesRecordException("Scartata linea malformattata, campo 'open' malformattato: " + record);
        }

        //prelevo il terzo campo (close) e ne verifico la correttezza
        float close;
        try {
            close = Float.parseFloat(stringTokenizer.nextToken());
        }catch (NumberFormatException e){
            throw new ParseStockPricesRecordException("Scartata linea malformattata, campo 'close' malformattato: " + record);
        }

        //prelevo il quarto campo (adj_close) e ne verifico la correttezza
        float adj_close;
        try {
            adj_close = Float.parseFloat( stringTokenizer.nextToken());
        }catch (NumberFormatException e){
            throw new ParseStockPricesRecordException("Scartata linea malformattata, campo 'adj_close' malformattato: " + record);
        }

        //prelevo il quinto campo (low) e ne verifico la correttezza
        float low;
        try {
            low = Float.parseFloat(stringTokenizer.nextToken());
        }catch (NumberFormatException e){
            throw new ParseStockPricesRecordException("Scartata linea malformattata, campo 'low' malformattato: " + record);
        }

        //prelevo il sesto campo (high) e ne verifico la correttezza
        float high;
        try {
            high = Float.parseFloat(stringTokenizer.nextToken());
        }catch (NumberFormatException e){
            throw new ParseStockPricesRecordException("Scartata linea malformattata, campo 'high' malformattato: " + record);
        }

        //prelevo il settimo campo (volume) e ne verifico la correttezza
        float volume;
        try {
            volume = Float.parseFloat(stringTokenizer.nextToken());
        }catch (NumberFormatException e){
            throw new ParseStockPricesRecordException("Scartata linea malformattata, campo 'volume' malformattato: " + record);
        }

        //prelevo l'ottavo campo (date)
        String dateString = stringTokenizer.nextToken();
        Date date;
        //verifico la correttezza dell'ottavo campo
        try{
            date = new SimpleDateFormat("yyyy-MM-dd").parse(dateString);
        }catch (ParseException e){
            throw new ParseStockPricesRecordException("Scartata linea malformattata, campo 'date' malformattato: " + record);
        }

        //creo e ritorno l'oggetto StockPricesRecord
        return new StockPricesRecord(ticker, open, close, adj_close, low, high, volume, date);
    }
}