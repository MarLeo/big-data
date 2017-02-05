package org.hadoop.project.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by marti on 05/02/2017.
 */
public class StripesMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

    private MapWritable neighborsOccurence = new MapWritable ();
    private Text word = new Text ();


    /**
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int neighbors = context.getConfiguration ().getInt ( "neighbors", 2 );
        String[] tokens = value.toString ().split ( "\\s+" );
        if (tokens.length > 1) {
            for (int i = 0; i < tokens.length; i++) {
                word.set ( tokens[i] );
                neighborsOccurence.clear ();


                int start = (i - neighbors < 0) ? 0 : i - neighbors;
                int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
                for (int j = start; j <= end; j++) {
                    if (j == i) continue;
                    Text neighbor = new Text ( tokens[j] );
                    if (neighborsOccurence.containsKey ( neighbor )) {
                        IntWritable occurences = (IntWritable) neighborsOccurence.get ( neighbor );
                        occurences.set ( occurences.get () + 1 );
                    } else {
                        neighborsOccurence.put ( neighbor, new IntWritable ( 1 ) );
                    }
                }

                context.write ( word, neighborsOccurence );

            }
        }
    }


}
