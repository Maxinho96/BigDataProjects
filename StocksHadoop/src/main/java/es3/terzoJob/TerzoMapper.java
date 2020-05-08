package es3.terzoJob;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

@Slf4j
public class TerzoMapper extends Mapper<LongWritable, Text, Text, Text> {

    @SneakyThrows
    @Override
    public void map(LongWritable key, Text value, Context context) {

        String[] cols = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        //log.info(Arrays.toString(cols));

        //la chiave sono le tre variazioni medie
        String resultKey = "2016: " + cols[1] + ", 2017: " + cols[2] + ", 2018: " + cols[3];

        //il valore è il nome dell'azienda
        String resultValue = cols[0];

        context.write(new Text(resultKey), new Text(resultValue));
    }

}