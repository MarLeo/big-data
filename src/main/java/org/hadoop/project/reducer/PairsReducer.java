package org.hadoop.project.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.hadoop.project.model.TextPair;

import java.io.IOException;

/**
 * Created by marti on 05/02/2017.
 */
public class PairsReducer extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {

    private IntWritable total = new IntWritable ();


    @Override
    protected void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get ();
        }
        total.set ( count );
        context.write ( key, total );
    }


}
