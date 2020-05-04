package es1.primoJob;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import es1.customWritables.SelectedFieldsOfStockPricesRecordWritable;
import es1.customWritables.TextArrayWritable;

import java.text.SimpleDateFormat;
import java.util.Date;

@Slf4j
public class PrimoReducer extends Reducer<Text, SelectedFieldsOfStockPricesRecordWritable, Text, TextArrayWritable> {

	private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

	@SneakyThrows
	@Override
	public void reduce(Text key, Iterable<SelectedFieldsOfStockPricesRecordWritable> values, Context context) {

		//numero dei volumi contati per un certo ticker
		int totVolume = 0;
		//somma dei volumi di un ticker
		float sumVolume = 0;

		//data minore (corrisponde al primo giorno di quotazione)
		Date firstQuotDate;

		//data maggiore (corrisponde all'ultimo giorno di quotazione)
		Date lastQuotDate;

		//quotazione nella data minore
		float firstQuotClose;

		//quotazione nella data maggiore
		float lastQuotClose;

		//quotazione minima
		float minClose;

		//quotazione massima
		float maxClose;

		//i tre attributi dell'oggetto SelectedFieldsOfStockPricesRecordWritable ricevuto
		float closeField;
		float volumeField;
		Date dateField;

		if(!values.iterator().hasNext())
			return;

		//prendo il primo record fuori dal loop per inizializzare minClose, maxClose, firstQuot, lastQuot
		SelectedFieldsOfStockPricesRecordWritable firstValue = values.iterator().next();
		closeField = firstValue.getClose();
		volumeField = firstValue.getVolume();
		dateField = this.simpleDateFormat.parse(firstValue.getDate());

		firstQuotClose = closeField;
		lastQuotClose = closeField;

		minClose = closeField;
		maxClose = closeField;

		totVolume++;
		sumVolume += volumeField;

		firstQuotDate = dateField;
		lastQuotDate = dateField;

		for (SelectedFieldsOfStockPricesRecordWritable value: values) {

			closeField = value.getClose();
			volumeField = value.getVolume();
			dateField = this.simpleDateFormat.parse(value.getDate());

			if(dateField.compareTo(firstQuotDate) < 0) {
				firstQuotDate = dateField;
				firstQuotClose = closeField;
			}
			else if(dateField.compareTo(lastQuotDate) > 0 ) {
				lastQuotDate = dateField;
				lastQuotClose = closeField;
			}

			if(closeField < minClose)
				minClose = closeField;
			else if(closeField > maxClose)
				maxClose = closeField;

			totVolume++;
			sumVolume += volumeField;
		}

		//variazione quotazione percentuale arrotondata per difetto
		int diffPercQuotRoundedDown = (int) (((lastQuotClose - firstQuotClose) / firstQuotClose)*100);
		//float diffPercQuotRoundedDown = ((lastQuotClose - firstQuotClose) / firstQuotClose)*100;

		//volume medio
		//arrivati qui non è possibile che totVolume sia 0
		float avgVolume = sumVolume/totVolume;

		String[] result = new String[4];
		result[0] = String.valueOf(diffPercQuotRoundedDown);
		result[1] = String.valueOf(minClose);
		result[2] = String.valueOf(maxClose);
		result[3] = String.valueOf(avgVolume);

		context.write(key, new TextArrayWritable(result));
	}
}