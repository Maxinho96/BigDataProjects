package es3.secondoJob;

import es3.customWritables.FloatArrayWritable;
import es3.customWritables.TextArrayWritable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

@Slf4j
public class SecondoReducer extends Reducer<Text, TextArrayWritable, Text, FloatArrayWritable> {

	private static final String TAB_VARIAZIONI = "variazioni";
	private static final String TAB_AZIENDE = "aziende";

	@SneakyThrows
	@Override
	public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) {

		String nomeAzienda = null;
		boolean trovataVariazione = false;

		float variazione2016 = 0;
		float variazione2017 = 0;
		float variazione2018 = 0;


		for (TextArrayWritable value: values) {
			String[] cols = value.toStrings();

			if(cols[0].equals(TAB_AZIENDE)){
				if(nomeAzienda != null && !nomeAzienda.equals(cols[0]))
					return; //ho trovato due nomi diversi per la stessa azienda
				nomeAzienda = cols[1]; //salvo il nome dell'azienda
			}
			else if(cols[0].equals(TAB_VARIAZIONI)){
				if(trovataVariazione)
					return; // ho trovato più di una variazione per lo stesso ticker allora riga MALFORMATTATA
				else
					trovataVariazione = true;

				try {
					variazione2016 = Float.parseFloat(cols[1]); //salvo la variazione del 2016
					variazione2017 = Float.parseFloat(cols[2]); //salvo la variazione del 2017
					variazione2018 = Float.parseFloat(cols[3]); //salvo la variazione del 2018
				}catch (NumberFormatException e){
					return; //riga malformattata
				}
			}
		}

		//controllo se ho trovato almeno una linea per tabella
		if(nomeAzienda == null || !trovataVariazione) {
			//log.info("non ho trovato abbastanza");
			return;
		}


		float[] result = new float[3];
		result[0] = variazione2016;
		result[1] = variazione2017;
		result[2] = variazione2018;


		context.write(new Text(nomeAzienda), new FloatArrayWritable(result));
	}
}