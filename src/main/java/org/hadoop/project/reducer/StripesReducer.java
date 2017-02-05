package org.hadoop.project.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;

/**
 * Created by marti on 05/02/2017.
 */
public class StripesReducer extends Reducer<Text, MapWritable, Text, MapWritable> {

    private MapWritable occurences = new MapWritable ();

    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        occurences.clear ();
        for (MapWritable value : values) {
            addOccurences ( value );
        }
        context.write ( key, occurences );
    }


    private void addOccurences(MapWritable value) {
        Set<Writable> keys = value.keySet ();
        for (Writable key : keys) {
            IntWritable itemCount = (IntWritable) value.get ( key );
            if (occurences.containsKey ( key )) {
                IntWritable count = (IntWritable) occurences.get ( key );
                count.set ( itemCount.get () + count.get () );
            } else {
                occurences.put ( key, itemCount );
            }
        }
    }


}
