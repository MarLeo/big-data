package org.hadoop.project.helpers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import java.util.Set;

/**
 * Created by marti on 14/02/2017.
 */
public class CustomMapWritable extends MapWritable {

    public CustomMapWritable() {
        super ();
    }

    @Override
    public String toString() {
        String word = new String ( "{" );
        Set<Writable> keys = this.keySet ();
        for (Writable key : keys) {
            IntWritable count = (IntWritable) this.get ( key );
            word = word + key.toString () + " = " + count.toString () + ",";
        }
        word = word + " }";

        return word;
    }
}
