package org.hadoop.project.helpers;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * Created by marti on 04/02/2017.
 */
public class RandomInputFormat implements InputFormat<Text, Text> {


    public InputSplit[] getSplits(JobConf jobConf, int splits) throws IOException {
        InputSplit[] result = new InputSplit[splits];
        Path outputDir = FileOutputFormat.getOutputPath ( jobConf );
        for (int i = 0; i < result.length; i++) {
            result[i] = new FileSplit ( new Path ( outputDir, "dummy-split-" + i ), 0, 1, (String[]) null );
        }
        return result;
    }

    public RecordReader<Text, Text> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        return new RandomRecordReader ( ((FileSplit) inputSplit).getPath () );
    }
}
