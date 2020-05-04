package es3;

import es3.customWritables.FloatArrayWritable;
import es3.customWritables.SelectedFieldsOfStockPricesRecordWritable;
import es3.customWritables.TextArrayWritable;
import es3.primoJob.PrimoMapper;
import es3.primoJob.PrimoReducer;
import es3.quartoJob.QuartoMapper;
import es3.quartoJob.QuartoReducer;
import es3.secondoJob.SecondoMapper;
import es3.secondoJob.SecondoReducer;
import es3.terzoJob.TerzoMapper;
import es3.terzoJob.TerzoReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Es3 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Path inputFilePathHistoricalStocksPrices = new Path(args[0]);
        Path inputFilePathHistoricalStocks = new Path(args[1]);
        Path tempUnoOutputFilePath = new Path("/tmp1/" + args[2]);
        Path tempDueOutputFilePath = new Path("/tmp2/" + args[2]);
        Path tempTreOutputFilePath = new Path("/tmp3/" + args[2]);
        Path finalOutputFilePath = new Path("/final/" + args[2]);

        //JOB 1

        Configuration configurationJob1 = new Configuration();
        //utilizzo la virgola come separatore tra key e value nell'output del job1
        configurationJob1.set("mapreduce.output.textoutputformat.separator", ",");

        Job job1 = Job.getInstance(configurationJob1, "Job1");

        job1.setJarByClass(Es3.class);

        FileInputFormat.addInputPath(job1, inputFilePathHistoricalStocksPrices);
        FileOutputFormat.setOutputPath(job1, tempUnoOutputFilePath);

        job1.setMapperClass(PrimoMapper.class);
        job1.setReducerClass(PrimoReducer.class);

        job1.setMapOutputKeyClass(Text.class); //key prodotta dal map
        job1.setMapOutputValueClass(SelectedFieldsOfStockPricesRecordWritable.class); //value prodotto dal map

        job1.setOutputKeyClass(Text.class); //key prodotta dal reduce (se non specifico quella del map anche lui usa questa)
        job1.setOutputValueClass(TextArrayWritable.class); //value prodotto dal reduce

        //job1.setNumReduceTasks(5);

        if (!job1.waitForCompletion(true)) {
            System.out.println("Job1 failed, exiting");
            System.exit(-1);
        }


        //JOB 2

        Configuration configurationJob2 = new Configuration();
        //utilizzo la virgola come separatore tra key e value nell'output del job2
        configurationJob2.set("mapreduce.output.textoutputformat.separator", ",");

        Job job2 = Job.getInstance(configurationJob2, "Job2");

        job2.setJarByClass(Es3.class);

        FileInputFormat.addInputPath(job2, tempUnoOutputFilePath);
        FileInputFormat.addInputPath(job2, inputFilePathHistoricalStocks);
        FileOutputFormat.setOutputPath(job2, tempDueOutputFilePath);

        job2.setMapperClass(SecondoMapper.class);
        job2.setReducerClass(SecondoReducer.class);

        job2.setMapOutputKeyClass(Text.class); //key prodotta dal map
        job2.setMapOutputValueClass(TextArrayWritable.class); //value prodotto dal map

        job2.setOutputKeyClass(Text.class); //key prodotta dal reduce (se non specifico quella del map anche lui usa questa)
        job2.setOutputValueClass(TextArrayWritable.class); //value prodotto dal reduce

        //job2.setInputFormatClass(KeyValueTextInputFormat.class);

        //job2.setNumReduceTasks(1);

        if (!job2.waitForCompletion(true)) {
            System.out.println("Job2 failed, exiting");
            System.exit(-1);
        }


        //JOB 3

        Configuration configurationJob3 = new Configuration();
        //utilizzo la virgola come separatore tra key e value nell'output del job3
        configurationJob3.set("mapreduce.output.textoutputformat.separator", ",");

        Job job3 = Job.getInstance(configurationJob3, "Job3");

        job3.setJarByClass(Es3.class);

        FileInputFormat.addInputPath(job3, tempDueOutputFilePath);
        FileOutputFormat.setOutputPath(job3, tempTreOutputFilePath);

        job3.setMapperClass(TerzoMapper.class);
        job3.setReducerClass(TerzoReducer.class);

        job3.setMapOutputKeyClass(Text.class); //key prodotta dal map
        job3.setMapOutputValueClass(FloatArrayWritable.class); //value prodotto dal map

        job3.setOutputKeyClass(Text.class); //key prodotta dal reduce (se non specifico quella del map anche lui usa questa)
        job3.setOutputValueClass(TextArrayWritable.class); //value prodotto dal reduce

        //job3.setInputFormatClass(KeyValueTextInputFormat.class);

        //job3.setNumReduceTasks(1);

        if (!job3.waitForCompletion(true)) {
            System.out.println("Job3 failed, exiting");
            System.exit(-1);
        }


        //JOB 4

        Job job4 = Job.getInstance(new Configuration(), "Job4");

        job4.setJarByClass(Es3.class);

        FileInputFormat.addInputPath(job4, tempTreOutputFilePath);
        FileOutputFormat.setOutputPath(job4, finalOutputFilePath);

        job4.setMapperClass(QuartoMapper.class);
        job4.setReducerClass(QuartoReducer.class);

        job4.setMapOutputKeyClass(Text.class); //key prodotta dal map
        job4.setMapOutputValueClass(Text.class); //value prodotto dal map

        job4.setOutputKeyClass(Text.class); //key prodotta dal reduce (se non specifico quella del map anche lui usa questa)
        job4.setOutputValueClass(Text.class); //value prodotto dal reduce

        //job4.setInputFormatClass(KeyValueTextInputFormat.class);

        //job4.setNumReduceTasks(1);

        if (!job4.waitForCompletion(true)) {
            System.out.println("Job4 failed, exiting");
            System.exit(-1);
        }

        //arrivato qui è andato tutto bene
        System.exit(0);
    }
}

