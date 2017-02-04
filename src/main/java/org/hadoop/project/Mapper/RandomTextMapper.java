package org.hadoop.project.Mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.hadoop.project.Counters.Counters;
import org.hadoop.project.Helpers.Helper;

import java.io.IOException;
import java.util.Random;

/**
 * Created by marti on 04/02/2017.
 */
public class RandomTextMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

    private long numBytesToWrite = (long) (30 * Math.pow ( 1024, 2 ));
    private int minWordsInKey = 5;
    private int wordsinKeyRange = 10 - minWordsInKey;
    private int minWordsInValue = 10;
    private int wordsInValueRange = 100 - minWordsInValue;
    private Random random = new Random ();


    public void map(Text key, Text value,
                    OutputCollector<Text, Text> output,
                    Reporter reporter) throws IOException {

        int items = 0;
        while (numBytesToWrite > 0) {
            // Generate the key/value
            int numWordsKey = minWordsInKey + (wordsinKeyRange != 0 ? random.nextInt ( wordsinKeyRange ) : 0);
            int numWordsValue = minWordsInValue + (wordsInValueRange != 0 ? random.nextInt ( wordsInValueRange ) : 0);
            Text keyWords = Helper.generateSentence ( numWordsKey, random );
            Text valueWords = Helper.generateSentence ( numWordsValue, random );

            // write the sentence
            output.collect ( keyWords, valueWords );

            numBytesToWrite -= (keyWords.getLength () + valueWords.getLength ());

            // Update counters, progress etc.
            //context.getCounter ( Counters.BYTES_WRITTEN).increment ( valueWords.getLength () );
            //context.getCounter ( Counters.RECORDS_WRITTEN ).increment ( 1 );
            reporter.incrCounter ( Counters.BYTES_WRITTEN, (keyWords.getLength () + valueWords.getLength ()) );
            reporter.incrCounter ( Counters.RECORDS_WRITTEN, 1 );
            if (++items % 200 == 0) {
                //context.setStatus ( String.format ( "Wrote %d . %d bytes left", items, numBytesToWrite ) );
                reporter.setStatus ( String.format ( "Wrote %d . %d bytes left", items, numBytesToWrite ) );
            }
        }
        reporter.setStatus ( String.format ( "Done with %d records", items ) );

    }


}
