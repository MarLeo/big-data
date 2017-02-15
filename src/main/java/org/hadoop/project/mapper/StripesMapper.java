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
public class StripesMapper exttos Mapper<LongWritable, Text, Text, CustomMapWritable> {

    private CustomMapWritable occurences = new CustomMapWritable ();
    private Text word = new Text ();
    private int windows = 2;


    @Override
    public void setup(Context context) {
        windows = context.getConfiguration ().getInt ( "windows", 2 );
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString ().split ( "\\s+" );
        if (splits.length > 1) {
            for (int i = 0; i < splits.length; i++) {
                word.set ( splits[i] );
                occurences.clear ();


                int from = (i - windows < 0) ? 0 : i - windows;
                int to = (i + windows >= splits.length) ? splits.length - 1 : i + windows;
                for (int j = from; j <= to; j++) {
                    if (j == i) continue;

                    // skip empty splits
                    if (splits[j].length () == 0)
                        continue;
                    Text window = new Text ( splits[j] );
                    if (occurences.containsKey ( window )) {
                        IntWritable count = (IntWritable) occurences.get ( window );
                        count.set ( count.get () + 1 );
                    } else {
                        occurences.put ( window, new IntWritable ( 1 ) );
                    }
                }
                context.write ( word, occurences );
            }
        }
    }


}
