package job2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job2 {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration()
        Job job = Job.getInstance(conf, "Job2");

        job.setJarByClass(Job2.class);

        job.setReducerClass(Reducer.class);
        // L'output deve avere come chiave la coppia settore, anno
        job.setOutputKeyClass(Text.class);
        // L'output deve avere come valore la tripla volume annuale medio, variazione annuale media, e quotazione
        // giornaliera media.
        job.setOutputValueClass(Text.class);

        // Si può mettere un job intermedio che faccia solo il join e abbia come mapper quello scritto sotto e come
        // reducer uno che dia in output il risultato del join (Fa tutte le possibili coppie tra settore e altri dati)
        // Poi si fa un altro job che fa le aggregazioni (da vedere come).

        // Questo mapper darà in output come chiave il ticker e come valore la lista composta da:
        // - identificativo che indica che il valore viene da StockPrices
        // - data (punti a, b e c)
        // - volume (punto a)
        // - prezzo di chiusura (punti b e c)
        // La lista può essere fatta con Text o con un tipo custom.
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, StockPricesMapper.class);
        // Questo mapper darà in output come chiave il ticker e come valore la lista composta da:
        // - identificativo che indica che il valore viene da Stocks
        // - settore
        // La lista può essere fatta con Text o con un tipo custom.
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, StocksMapper.class);
        Path outputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);

        job.waitForCompletion(true);

}
