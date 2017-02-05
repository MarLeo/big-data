package org.hadoop.project.model;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * Created by marti on 05/02/2017.
 */
public class WordPairPartitioner extends Partitioner<WordPair, IntWritable> {

    public int getPartition(WordPair wordPair, IntWritable intWritable, int partitions) {
        return wordPair.getWord ().hashCode () % partitions;
    }

}
