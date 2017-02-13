package org.hadoop.project.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.hadoop.project.model.WordPair;

import java.io.IOException;

/**
 * Created by marti on 05/02/2017.
 */
public class PairsMapper extends Mapper<LongWritable, Text, WordPair, IntWritable> {

    private WordPair wordPair = new WordPair ();
    private IntWritable ONE = new IntWritable ( 1 );


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int neighbors = context.getConfiguration ().getInt ( "voisins", 2 );
        String[] tokens = value.toString ().split ( "\\s+" );
        if (tokens.length > 1) {
            for (int i = 0; i < tokens.length; i++) {
                wordPair.setWord ( new Text ( tokens[i] ) );

                int start = (i - neighbors < 0) ? 0 : i - neighbors;
                int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
                for (int j = start; j <= end; j++) {
                    if (j == i) continue;
                    wordPair.setNeighbor ( new Text ( tokens[j] ) );
                    context.write ( wordPair, ONE );
                }
            }
        }

    }


}
