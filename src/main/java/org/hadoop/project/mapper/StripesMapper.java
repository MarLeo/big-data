package org.hadoop.project.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.hadoop.project.helpers.CustomMapWritable;

import java.io.IOException;

/**
 * Created by marti on 05/02/2017.
 */
public class StripesMapper extends Mapper<LongWritable, Text, Text, CustomMapWritable> {

    private CustomMapWritable occurences = new CustomMapWritable ();
    private Text word = new Text ();
    private int neighbors = 2;


    @Override
    public void setup(Context context) {
        neighbors = context.getConfiguration ().getInt ( "neighbors", 2 );
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //int voisins = context.getConfiguration ().getInt ( "voisins", 2 );
        String[] tokens = value.toString ().split ( "\\s+" );
        if (tokens.length > 1) {
            for (int i = 0; i < tokens.length; i++) {
                word.set ( tokens[i] );
                occurences.clear ();


                int start = (i - neighbors < 0) ? 0 : i - neighbors;
                int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
                for (int j = start; j <= end; j++) {
                    if (j == i) continue;

                    // skip empty tokens
                    if (tokens[j].length () == 0)
                        continue;
                    Text neighbor = new Text ( tokens[j] );
                    if (occurences.containsKey ( neighbor )) {
                        IntWritable count = (IntWritable) occurences.get ( neighbor );
                        count.set ( count.get () + 1 );
                    } else {
                        occurences.put ( neighbor, new IntWritable ( 1 ) );
                    }
                }

                context.write ( word, occurences );

            }
        }
    }


}
