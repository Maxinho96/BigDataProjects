package es3.terzoJob;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

@Slf4j
public class TerzoReducer extends Reducer<Text, Text, Text, Text> {

	@SneakyThrows
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) {

		//se non ho aziende scarto la linea
		if(!values.iterator().hasNext())
			return;

		String resultKey = "{";
		int totAziende = 0;

		for (Text value: values) {
			totAziende++;
			resultKey += value.toString() + ", ";
		}

		resultKey = resultKey.substring(0,resultKey.length()-2);
		resultKey += "}";

		if(totAziende>1)
			context.write(new Text(resultKey), key);
	}
}




