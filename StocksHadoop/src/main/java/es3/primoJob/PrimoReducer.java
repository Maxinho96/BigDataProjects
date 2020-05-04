package es3.primoJob;

import es3.customWritables.FloatArrayWritable;
import es3.customWritables.SelectedFieldsOfStockPricesRecordWritable;
import es3.customWritables.TextArrayWritable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.text.SimpleDateFormat;
import java.util.Date;

@Slf4j
public class PrimoReducer extends Reducer<Text, SelectedFieldsOfStockPricesRecordWritable, Text, FloatArrayWritable> {

	private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

	@SneakyThrows
	@Override
	public void reduce(Text key, Iterable<SelectedFieldsOfStockPricesRecordWritable> values, Context context) {

		//prima data utile di ogni anno
		Date firstQuotDate2016 = null; // = this.simpleDateFormat.parse("2017-01-01");
		Date firstQuotDate2017 = null; // = this.simpleDateFormat.parse("2018-01-01");
		Date firstQuotDate2018 = null; // = this.simpleDateFormat.parse("2019-01-01");

		//ultima data utile di ogni anno
		Date lastQuotDate2016 = null; // = this.simpleDateFormat.parse("2015-12-31");
		Date lastQuotDate2017 = null; // = this.simpleDateFormat.parse("2016-12-31");
		Date lastQuotDate2018 = null; // = this.simpleDateFormat.parse("2017-12-31");

		//quotazione nella prima data utile di ogni anno
		float firstQuotClose2016 = 0;
		float firstQuotClose2017 = 0;
		float firstQuotClose2018 = 0;

		//quotazione nell'ultima data utile di ogni anno
		float lastQuotClose2016 = 0;
		float lastQuotClose2017 = 0;
		float lastQuotClose2018 = 0;

		Date dateField;
		int year;

		for (SelectedFieldsOfStockPricesRecordWritable value: values) {

			dateField = this.simpleDateFormat.parse(value.getDate());
			year = dateField.getYear()+1900;

			switch (year){
				case 2016:

					if(firstQuotDate2016 == null && lastQuotDate2016 == null){
						firstQuotDate2016 = dateField;
						lastQuotDate2016 = dateField;
						firstQuotClose2016 = value.getClose();
						lastQuotClose2016 = value.getClose();
					}
					else if(dateField.compareTo(firstQuotDate2016) < 0){
						firstQuotDate2016 = dateField;
						firstQuotClose2016 = value.getClose();
					}
					else if(dateField.compareTo(lastQuotDate2016) > 0){
						lastQuotDate2016 = dateField;
						lastQuotClose2016 = value.getClose();
					}
					break;

				case 2017:

					if(firstQuotDate2017 == null && lastQuotDate2017 == null){
						firstQuotDate2017 = dateField;
						lastQuotDate2017 = dateField;
						firstQuotClose2017 = value.getClose();
						lastQuotClose2017 = value.getClose();
					}
					else if(dateField.compareTo(firstQuotDate2017) < 0){
						firstQuotDate2017 = dateField;
						firstQuotClose2017 = value.getClose();
					}
					else if(dateField.compareTo(lastQuotDate2017) > 0){
						lastQuotDate2017 = dateField;
						lastQuotClose2017 = value.getClose();
					}
					break;

				case 2018:

					if(firstQuotDate2018 == null && lastQuotDate2018 == null){
						firstQuotDate2018 = dateField;
						lastQuotDate2018 = dateField;
						firstQuotClose2018 = value.getClose();
						lastQuotClose2018 = value.getClose();
					}
					else if(dateField.compareTo(firstQuotDate2018) < 0){
						firstQuotDate2018 = dateField;
						firstQuotClose2018 = value.getClose();
					}
					else if(dateField.compareTo(lastQuotDate2018) > 0){
						lastQuotDate2018 = dateField;
						lastQuotClose2018 = value.getClose();
					}
					break;
			}
		}

		//in caso non abbia trovato almeno un valore per ogni anno
		// TODO sarebbe bello controllare di avere proprio i dati del primo e dell'ultimo giorno per assicurare che la variazione sia accurata
		//  (anche di azioni entrate o uscite dalla borsa nel mezzo di un anno)
		if(firstQuotClose2016 == 0 || lastQuotClose2016 == 0 || firstQuotClose2017 == 0 || lastQuotClose2017 == 0 || firstQuotClose2018 == 0
				|| lastQuotClose2018 == 0) {
			//log.info("Linea scartata perchè non è presente almeno un valore per anno");
			return;
		}

		//variazione quotazione percentuale di ogni anno
		float diffPercQuot2016 = ((lastQuotClose2016 - firstQuotClose2016) / firstQuotClose2016)*100;
		float diffPercQuot2017 = ((lastQuotClose2017 - firstQuotClose2017) / firstQuotClose2017)*100;
		float diffPercQuot2018 = ((lastQuotClose2018 - firstQuotClose2018) / firstQuotClose2018)*100;

		float[] result = new float[3];
		result[0] = diffPercQuot2016;
		result[1] = diffPercQuot2017;
		result[2] = diffPercQuot2018;

		context.write(key, new FloatArrayWritable(result));
	}
}