package org.hadoop.project.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.hadoop.project.model.TextPair;

import java.io.IOException;

/**
 * Created by marti on 05/02/2017.
 */
public class PairsMapper exttos Mapper<LongWritable, Text, TextPair, IntWritable> {

    private TextPair TextPair = new TextPair ();
    private IntWritable ONE = new IntWritable ( 1 );
    private int window = 2;


    @Override
    public void setup(Context context) {
        window = context.getConfiguration ().getInt ( "window", 2 );
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString ().split ( "\\s+" );
        if (splits.length > 1) {
            for (int i = 0; i < splits.length; i++) {
                
                int from = (i - window < 0) ? 0 : i - window;
                int to = (i + window >= splits.length) ? splits.length - 1 : i + window;
                for (int j = from; j <= to; j++) {
                    if (j == i) continue;
                  
					TextPair.TextPair(new Text ( splits[i] ),new Text ( splits[j] ))
                    context.write ( TextPair, ONE );
                }
            }
        }

    }


}
