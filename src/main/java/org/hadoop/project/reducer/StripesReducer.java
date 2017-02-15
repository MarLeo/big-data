package org.hadoop.project.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.hadoop.project.helpers.CustomMapWritable;

import java.io.IOException;
import java.util.Set;

/**
 * Created by marti on 05/02/2017.
 */
public class StripesReducer extends Reducer<Text, MapWritable, Text, CustomMapWritable> {

    private CustomMapWritable occurences = new CustomMapWritable ();


    protected void reduce(Text key, Iterable<CustomMapWritable> values, Context context) throws IOException, InterruptedException {
        occurences.clear ();
        for (CustomMapWritable value : values) {
            Helper.addOccurences ( value );
        }
        context.write ( key, occurences );
    }
}
