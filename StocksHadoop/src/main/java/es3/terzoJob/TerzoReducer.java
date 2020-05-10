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

		for (Text value: values)
			resultKey += value.toString() + ", ";

		resultKey = resultKey.substring(0,resultKey.length()-2);
		resultKey += "}";

		context.write(new Text(resultKey), key);
	}
}




