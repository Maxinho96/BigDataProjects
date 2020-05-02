package es1;

import es1.customSorts.FloatWritableDescendingSort;
import es1.customSorts.IntWritableDescendingSort;
import es1.customWritables.SelectedFieldsOfStockPricesRecordWritable;
import es1.customWritables.TextArrayWritable;
import es1.customWritables.TickerDataWithoutDiffPercQuotWritable;
import es1.primoJob.PrimoMapper;
import es1.primoJob.PrimoReducer;
import es1.secondoJob.SecondoMapper;
import es1.secondoJob.SecondoReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Es1 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Path inputFilePath = new Path(args[0]);
        Path tempOutputFilePath = new Path("/tmp/" + args[1]);
        Path finalOutputFilePath = new Path("/final/" + args[1]);


        //JOB 1

        Configuration configurationJob1 = new Configuration();
        //utilizzo la virgola come separatore tra key e value nell'output del job1
        configurationJob1.set("mapreduce.output.textoutputformat.separator", ",");

        Job job1 = Job.getInstance(configurationJob1, "Job1");

        job1.setJarByClass(Es1.class);

        FileInputFormat.addInputPath(job1, inputFilePath);
        FileOutputFormat.setOutputPath(job1, tempOutputFilePath);

        job1.setMapperClass(PrimoMapper.class);
        job1.setReducerClass(PrimoReducer.class);

        //job1.setMapOutputKeyClass(Text.class); //key prodotta dal map
        job1.setMapOutputValueClass(SelectedFieldsOfStockPricesRecordWritable.class); //value prodotto dal map

        job1.setOutputKeyClass(Text.class); //key prodotta dal //TODO controlla
        job1.setOutputValueClass(TextArrayWritable.class); //value prodotto dal reduce

        //job1.setNumReduceTasks(5);

        if (!job1.waitForCompletion(true)) {
            System.out.println("Job1 failed, exiting");
            System.exit(-1);
        }


        //JOB 2

        Job job2 = Job.getInstance(new Configuration(), "Job2");

        job2.setJarByClass(Es1.class);

        FileInputFormat.setInputPaths(job2, tempOutputFilePath); //TODO: perche setPaths e non addPath? perche con + reducer ho piu file??
        FileOutputFormat.setOutputPath(job2, finalOutputFilePath);

        job2.setMapperClass(SecondoMapper.class);
        job2.setReducerClass(SecondoReducer.class);

        job2.setMapOutputKeyClass(IntWritable.class); //key prodotta dal map
        job2.setMapOutputValueClass(TickerDataWithoutDiffPercQuotWritable.class); //value prodotto dal map

        job2.setOutputKeyClass(Text.class); //key prodotta dal //TODO controlla
        job2.setOutputValueClass(TextArrayWritable.class); //value prodotto dal reduce

        //utilizzo un comparator di IntWritable in ordine discendente nella fase di Shuffle and Sort
        job2.setSortComparatorClass(IntWritableDescendingSort.class);
        //ReverseComparator.class //TODO cos'è?

        //job2.setInputFormatClass(KeyValueTextInputFormat.class);

        //job2.setNumReduceTasks(1);

        if (!job2.waitForCompletion(true)) {
            System.out.println("Job2 failed, exiting");
            System.exit(-1);
        }

        //arrivato qui è andato tutto bene
        System.exit(0);
    }
}

