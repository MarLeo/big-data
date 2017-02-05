package org.hadoop.project.helpers;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

/**
 * Created by marti on 04/02/2017.
 */
public class RandomRecordReader implements RecordReader<Text, Text> {

    Path name;

    public RandomRecordReader(Path path) {
        name = path;
    }

    public boolean next(Text key, Text value) throws IOException {
        if (name != null) {
            key.set ( name.getName () );
            name = null;
            return true;
        }
        return false;
    }

    public Text createKey() {
        return new Text ();
    }

    public Text createValue() {
        return new Text ();
    }

    public long getPos() throws IOException {
        return 0;
    }

    public void close() throws IOException {

    }

    public float getProgress() throws IOException {
        return 0.0f;
    }
}
