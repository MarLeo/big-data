package org.hadoop.project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.hadoop.project.helpers.RandomInputFormat;
import org.hadoop.project.mapper.RandomTextMapper;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;


/**
 * Created by marti on 22/01/2017.
 */
public class Driver extends Configured implements Tool {

    static int printUsage() {
        System.out.println ( String.format ( "randomtextgenerator [-outputFormat <output format class>] <output>" ) );
        ToolRunner.printGenericCommandUsage(System.out);
        return 2;
    }

    public static void main(String[] args) throws Exception {
        final Logger LOGGER = LogManager.getLogger(Driver.class);
        LOGGER.info(String.format("Lauching %s at %s", Driver.class.getSimpleName(), new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(Calendar.getInstance().getTime())));
        int exitCode = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        if (args.length == 0) {
            return printUsage();
        }

        // Input Path
        //Path inPath = new Path ( args[0] );

        // Output Path
        Path outputDir = new Path ( args[0] );

        int numMaps = Integer.parseInt ( args[1] )/*10*/;

        // Create configuration
        Configuration conf = getConf();

        //Create job
        JobConf job = new JobConf ( conf, this.getClass () );
        job.setJarByClass(Driver.class);
        job.setJobName ( "Driver for hadoop random text generator" );

        // Setup MapReduce
        job.setMapperClass ( RandomTextMapper.class );
        job.setNumMapTasks ( numMaps );
        System.out.println ( "Running " + numMaps + " maps." );
        // reducer NONE
        job.setNumReduceTasks(0);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input
        job.setInputFormat ( RandomInputFormat.class );

        Class<? extends OutputFormat> outputFormatClass =
                SequenceFileOutputFormat.class;
        List<String> otherArgs = new ArrayList<String> ();
        for (int i = 0; i < args.length; ++i) {
            try {
                if ("-outFormat".equals ( args[i] )) {
                    outputFormatClass =
                            Class.forName ( args[++i] ).asSubclass ( OutputFormat.class );
                } else {
                    otherArgs.add ( args[i] );
                }
            } catch (ArrayIndexOutOfBoundsException except) {
                System.out.println ( "ERROR: Required parameter missing from " +
                        args[i - 1] );
                return printUsage (); // exits
            }
        }

        // Input
        //FileInputFormat.addInputPath ( job, inPath );

        // Output
        FileOutputFormat.setOutputPath ( job, outputDir );
        job.setOutputFormat ( outputFormatClass );

        // Delete output if exists
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir)) {
            hdfs.delete(outputDir, true);
        }

        // Execute job
        Date startTime = new Date();
        System.out.println("Job started : " + startTime);
        RunningJob runningJob = JobClient.runJob ( job );
        int ret = runningJob.getJobState ();//job.waitForCompletion(true) ? 0 : 1;
        Date endTime = new Date();
        System.out.println("Job ended: " + endTime);
        System.out.println("The job took " + (endTime.getTime() - startTime.getTime()) / 1000 + " seconds");
        return ret;
    }

}
