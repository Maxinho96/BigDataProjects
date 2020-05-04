package es3.terzoJob;

import es3.customWritables.FloatArrayWritable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

@Slf4j
public class TerzoMapper extends Mapper<LongWritable, Text, Text, FloatArrayWritable> {

    @SneakyThrows
    @Override
    public void map(LongWritable key, Text value, Context context) {

        String[] cols = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        //log.info(Arrays.toString(cols));

        //la chiave è il nome dell'azienda
        String resultKey = cols[0];

        //il valore sono le tre variazioni annue
        float[] resultValue = new float[3];
        try {
            resultValue[0] = Float.parseFloat(cols[1]);
            resultValue[1] = Float.parseFloat(cols[2]);
            resultValue[2] = Float.parseFloat(cols[3]);
        }catch (NumberFormatException e){
            return; //riga malformattata
        }

        //log.info("SCRIVO KEY: " + resultKey + " VALUE: " + Arrays.toString(resultValue));

        context.write(new Text(resultKey), new FloatArrayWritable(resultValue));
        //log.info("SCRITTO");
    }

}