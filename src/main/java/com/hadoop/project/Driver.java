package com.hadoop.project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by marti on 22/01/2017.
 */
public class Driver extends Configured implements Tool {

    static int printUsage() {
        System.out.println(String.format("randomdatagenerator [-outputFormat <output format class>] <output>"));
        ToolRunner.printGenericCommandUsage(System.out);
        return 2;
    }

    public static void main(String[] args) throws Exception {
        final Logger LOGGER = LogManager.getLogger(Driver.class);
        LOGGER.info(String.format("Lauching %s at %s", Driver.class.getSimpleName(), new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(Calendar.getInstance().getTime())));
        //System.out.println("Hello world!");
        int exitCode = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        if (args.length == 0) {
            return printUsage();
        }

        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(Driver.class);
        job.setJobName("Driver for hadoop random generator words");

        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // reducer NONE
        job.setNumReduceTasks(0);

        Date startTime = new Date();
        System.out.println("Job started : " + startTime);
        int ret = job.waitForCompletion(true) ? 0 : 1;
        Date endTime = new Date();
        System.out.println("Job ended: " + endTime);
        System.out.println("The job took " + (endTime.getTime() - startTime.getTime()) / 1000 + " seconds");
        return ret;
    }

}
