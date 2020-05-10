package es3;

import es3.customWritables.FloatArrayWritable;
import es3.customWritables.TextArrayWritable;
import es3.primoJob.PrimoMapperAziende;
import es3.primoJob.PrimoMapperVariazioni;
import es3.primoJob.PrimoReducer;
import es3.terzoJob.TerzoMapper;
import es3.terzoJob.TerzoReducer;
import es3.secondoJob.SecondoMapper;
import es3.secondoJob.SecondoReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Es3 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Path inputFilePathHistoricalStocksPrices = new Path(args[0]);
        Path inputFilePathHistoricalStocks = new Path(args[1]);
        Path tempUnoOutputFilePath = new Path("/tmp1/" + args[2]); //output del job1
        Path tempDueOutputFilePath = new Path("/tmp2/" + args[2]); //output del job2
        //Path tempTreOutputFilePath = new Path("/tmp3/" + args[2]);
        Path finalOutputFilePath = new Path("/final/" + args[2]); //output del job2

        //JOB 1

        Configuration configurationJob1 = new Configuration();
        //utilizzo la virgola come separatore tra key e value nell'output del job1
        configurationJob1.set("mapreduce.output.textoutputformat.separator", ",");

        Job job1 = Job.getInstance(configurationJob1, "Job1");

        job1.setJarByClass(Es3.class);

        MultipleInputs.addInputPath(job1, inputFilePathHistoricalStocks, TextInputFormat.class, PrimoMapperAziende.class);
        MultipleInputs.addInputPath(job1, inputFilePathHistoricalStocksPrices, TextInputFormat.class, PrimoMapperVariazioni.class);

        FileOutputFormat.setOutputPath(job1, tempUnoOutputFilePath);

        job1.setReducerClass(PrimoReducer.class);

        job1.setMapOutputKeyClass(Text.class); //key prodotta dal map
        job1.setMapOutputValueClass(TextArrayWritable.class); //value prodotto dal map

        job1.setOutputKeyClass(Text.class); //key prodotta dal reduce (se non specifico quella del map anche lui usa questa)
        job1.setOutputValueClass(FloatArrayWritable.class); //value prodotto dal reduce

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
        FileOutputFormat.setOutputPath(job2, tempDueOutputFilePath);

        job2.setMapperClass(SecondoMapper.class);
        job2.setReducerClass(SecondoReducer.class);

        job2.setMapOutputKeyClass(Text.class); //key prodotta dal map
        job2.setMapOutputValueClass(FloatArrayWritable.class); //value prodotto dal map

        job2.setOutputKeyClass(Text.class); //key prodotta dal reduce (se non specifico quella del map anche lui usa questa)
        job2.setOutputValueClass(TextArrayWritable.class); //value prodotto dal reduce

        //job2.setInputFormatClass(KeyValueTextInputFormat.class);

        //job2.setNumReduceTasks(1);

        if (!job2.waitForCompletion(true)) {
            System.out.println("Job2 failed, exiting");
            System.exit(-1);
        }


        //JOB 3

        Job job3 = Job.getInstance(new Configuration(), "Job3");

        job3.setJarByClass(Es3.class);

        FileInputFormat.addInputPath(job3, tempDueOutputFilePath);
        FileOutputFormat.setOutputPath(job3, finalOutputFilePath);

        job3.setMapperClass(TerzoMapper.class);
        job3.setReducerClass(TerzoReducer.class);

        job3.setMapOutputKeyClass(Text.class); //key prodotta dal map
        job3.setMapOutputValueClass(Text.class); //value prodotto dal map

        job3.setOutputKeyClass(Text.class); //key prodotta dal reduce (se non specifico quella del map anche lui usa questa)
        job3.setOutputValueClass(Text.class); //value prodotto dal reduce

        //job3.setInputFormatClass(KeyValueTextInputFormat.class);

        //job3.setNumReduceTasks(1);

        if (!job3.waitForCompletion(true)) {
            System.out.println("Job3 failed, exiting");
            System.exit(-1);
        }

        //arrivato qui è andato tutto bene
        System.exit(0);
    }
}

