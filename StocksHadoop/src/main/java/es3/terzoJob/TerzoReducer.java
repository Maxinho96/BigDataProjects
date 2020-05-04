package es3.terzoJob;

import es3.customWritables.FloatArrayWritable;
import es3.customWritables.TextArrayWritable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.Arrays;

@Slf4j
public class TerzoReducer extends Reducer<Text, FloatArrayWritable, Text, TextArrayWritable> {

	@SneakyThrows
	@Override
	public void reduce(Text key, Iterable<FloatArrayWritable> values, Context context) {

		//se non ho variaizoni scarto la linea
		if(!values.iterator().hasNext())
			return;

		float sumVariazioni2016 = 0;
		float sumVariazioni2017 = 0;
		float sumVariazioni2018 = 0;
		int totTickers = 0;

		for (FloatArrayWritable value: values) {
			//log.info("value: " + Arrays.toString(value.toStrings()));
			String[] cols = value.toStrings();
			//log.info("COLONNA: " + Arrays.toString(cols));
			try {
				sumVariazioni2016 += Float.parseFloat(cols[0]); //sommo le variazione del 2016
				sumVariazioni2017 += Float.parseFloat(cols[1]); //sommo le variazione del 2017
				sumVariazioni2018 += Float.parseFloat(cols[2]); //sommo le variazione del 2018
				totTickers++; //ho quindi trovato un nuovo ticker per la stessa azienda
			}catch (NumberFormatException e){
				return; //riga malformattata
			}
		}

		String[] result = new String[3];

		int mediaVariazioni2016RoundedDown = (int) (sumVariazioni2016 / totTickers);
		int mediaVariazioni2017RoundedDown = (int) (sumVariazioni2017 / totTickers);
		int mediaVariazioni2018RoundedDown = (int) (sumVariazioni2018 / totTickers);

		result[0] = String.valueOf(mediaVariazioni2016RoundedDown);
		result[1] = String.valueOf(mediaVariazioni2017RoundedDown);
		result[2] = String.valueOf(mediaVariazioni2018RoundedDown);

		context.write(key, new TextArrayWritable(result));

	}
}




