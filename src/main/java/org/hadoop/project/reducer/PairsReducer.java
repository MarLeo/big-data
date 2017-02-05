package org.hadoop.project.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.hadoop.project.model.WordPair;

import java.io.IOException;

/**
 * Created by marti on 05/02/2017.
 */
public class PairsReducer extends Reducer<WordPair, IntWritable, WordPair, IntWritable> {

    //private IntWritable total = new IntWritable (  );


    @Override
    protected void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get ();
        }
        context.write ( key, new IntWritable ( count ) );
    }


}
