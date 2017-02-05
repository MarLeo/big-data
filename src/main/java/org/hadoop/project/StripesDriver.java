package org.hadoop.project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.hadoop.project.mapper.StripesMapper;
import org.hadoop.project.reducer.StripesReducer;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by marti on 05/02/2017.
 */
public class StripesDriver extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        final Logger LOGGER = LogManager.getLogger ( StripesDriver.class );
        LOGGER.info ( String.format ( "Lauching %s at %s", StripesDriver.class.getSimpleName (), new SimpleDateFormat ( "dd/MM/yyyy HH:mm:ss" ).format ( Calendar.getInstance ().getTime () ) ) );
        int exitCode = ToolRunner.run ( new Configuration (), new StripesDriver (), args );
        System.exit ( exitCode );
    }


    public int run(String[] args) throws Exception {
        if (args.length == 0) {
            return printUsage ();
        }

        // Input Path
        Path inputPath = new Path ( args[0] );

        // Output Path
        Path outputPath = new Path ( args[1] );

        // Create Configuration
        Configuration conf = new Configuration ();

        // Create Job
        Job job = new Job ( conf );
        job.setJarByClass ( StripesDriver.class );
        job.setJobName ( "Stripes co-occurence driver" );

        // Setup output
        job.setOutputKeyClass ( Text.class );
        job.setOutputValueClass ( MapWritable.class );

        // Setup mapper
        job.setMapperClass ( StripesMapper.class );

        // Setup reducer
        job.setReducerClass ( StripesReducer.class );


        // Setup Combiner
        job.setCombinerClass ( StripesReducer.class );

        // Input
        FileInputFormat.addInputPath ( job, inputPath );

        // output
        FileOutputFormat.setOutputPath ( job, outputPath );

        // Delete output if exists
        FileSystem hdfs = FileSystem.get ( conf );
        if (hdfs.exists ( outputPath )) {
            hdfs.delete ( outputPath, true );
        }

        // Execute job
        Date startTime = new Date ();
        System.out.println ( "Job started : " + startTime );
        int ret = job.waitForCompletion ( true ) ? 0 : 1;
        Date endTime = new Date ();
        System.out.println ( "Job ended: " + endTime );
        System.out.println ( "The job took " + (endTime.getTime () - startTime.getTime ()) / 1000 + " seconds" );
        return ret;
    }

    static int printUsage() {
        System.out.println ( String.format ( "word co-occurences [-outputFormat <output format class>] <output>" ) );
        ToolRunner.printGenericCommandUsage ( System.out );
        return 2;
    }
}
