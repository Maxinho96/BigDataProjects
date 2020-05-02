package es2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Es2 {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Job1");
        job1.setJarByClass(Es2.class);

        // Questo mapper darà in output come chiave il ticker e come valore la lista composta da:
        // - identificativo che indica che il valore viene da StockPrices
        // - data (punti a, b e c)
        // - volume (punto a)
        // - prezzo di chiusura (punti b e c)
        // La lista può essere fatta con Text o con un tipo custom.
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, StockPricesJoinMapper.class);
        // Questo mapper darà in output come chiave il ticker e come valore la lista composta da:
        // - identificativo che indica che il valore viene da Stocks
        // - settore
        // La lista può essere fatta con Text o con un tipo custom.
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, StocksJoinMapper.class);

        job1.setReducerClass(JoinReducer.class);
        // Questo reducer ha come chiave il ticker e come valore tute le possibili combinazioni tra settore (che viene
        // da Stocks) e gli altri campi (che vengono da StockPrices).
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        Path job1OutputPath = new Path("output/es2Job1Output");
        FileOutputFormat.setOutputPath(job1, job1OutputPath);
        job1OutputPath.getFileSystem(conf).delete(job1OutputPath);

        job1.waitForCompletion(true);


        Job job2 = Job.getInstance(conf, "Job2");
        job2.setJarByClass(Es2.class);

        FileInputFormat.setInputPaths(job2, job1OutputPath);

        // Questo mapper deve avere come chiave la coppia settore, anno e come valori la lista composta da:
        // - giorno e mese (o anche data completa)
        // - volume (punto a)
        // - prezzo di chiusura (punti b e c)
        // - ticker (punto b)
        job2.setMapperClass(AggregationsMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setReducerClass(AggregationsReducer.class);
        // L'output deve avere come chiave la coppia settore, anno
        job2.setOutputKeyClass(Text.class);
        // L'output deve avere come valore la tripla volume annuale medio, variazione annuale media, e quotazione
        // giornaliera media.
        job2.setOutputValueClass(Text.class);

        Path job2OutputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job2, job2OutputPath);
        job2OutputPath.getFileSystem(conf).delete(job2OutputPath);

        job2.waitForCompletion(true);

    }
}
