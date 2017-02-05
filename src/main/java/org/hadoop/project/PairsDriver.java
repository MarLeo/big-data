package org.hadoop.project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.hadoop.project.mapper.PairsMapper;
import org.hadoop.project.model.WordPair;
import org.hadoop.project.reducer.PairsReducer;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by marti on 05/02/2017.
 */
public class PairsDriver extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        final Logger LOGGER = LogManager.getLogger ( PairsDriver.class );
        LOGGER.info ( String.format ( "Lauching %s at %s", PairsDriver.class.getSimpleName (), new SimpleDateFormat ( "dd/MM/yyyy HH:mm:ss" ).format ( Calendar.getInstance ().getTime () ) ) );
        int exitCode = ToolRunner.run ( new Configuration (), new PairsDriver (), args );
        System.exit ( exitCode );
    }


    public int run(String[] args) throws Exception {

        if (args.length == 0) {
            return printUsage ();
        }

        // Input Path
        Path inPath = new Path ( args[0] );

        // Output Path
        Path outPath = new Path ( args[1] );

        // Create Configuration
        Configuration conf = new Configuration ();

        // Create Job
        Job job = new Job ( conf );
        job.setJarByClass ( PairsDriver.class );
        job.setJobName ( "Pairs co-occurence driver" );

        // Setup output
        job.setOutputKeyClass ( WordPair.class );
        job.setOutputValueClass ( IntWritable.class );

        // Setup mapper
        job.setMapperClass ( PairsMapper.class );

        // Setup reducer
        job.setReducerClass ( PairsReducer.class );


        // Setup Combiner
        job.setCombinerClass ( PairsReducer.class );

        // Input
        FileInputFormat.addInputPath ( job, inPath );

        // output
        FileOutputFormat.setOutputPath ( job, outPath );

        // Delete output if exists
        FileSystem hdfs = FileSystem.get ( conf );
        if (hdfs.exists ( outPath )) {
            hdfs.delete ( outPath, true );
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
