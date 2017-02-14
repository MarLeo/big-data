package org.hadoop.project.model;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by marti on 05/02/2017.
 */
public class WordPair implements Writable, WritableComparable<WordPair> {

    private Text word;
    private Text neighbor;
    private String delimiter = " ";

    public WordPair(Text word, Text neighbor) {
        this.word = word;
        this.neighbor = neighbor;
    }

    public WordPair(String word, String neighbor) {
        this ( new Text ( word ), new Text ( neighbor ) );
    }

    public WordPair() {
        this.word = new Text ();
        this.neighbor = new Text ();
    }


    public int compareTo(WordPair wordPair) {
        int ret = this.word.compareTo ( wordPair.getWord () );
        if (ret != 0) {
            return ret;
        }
        if (this.neighbor.toString ().equals ( "*" )) {
            return -1;
        } else if (wordPair.getNeighbor ().equals ( "*" )) {
            return 1;
        }
        return this.neighbor.compareTo ( wordPair.getNeighbor () );
    }

    public void write(DataOutput dataOutput) throws IOException {
        word.write ( dataOutput );
        neighbor.write ( dataOutput );

    }

    public void readFields(DataInput dataInput) throws IOException {
        word.readFields ( dataInput );
        neighbor.readFields ( dataInput );
    }

    public static WordPair read(DataInput dataInput) throws IOException {
        WordPair wordPair = new WordPair ();
        wordPair.readFields ( dataInput );
        return wordPair;
    }


    public Text getWord() {
        return word;
    }

    public void setWord(Text word) {
        this.word = word;
    }

    public Text getNeighbor() {
        return neighbor;
    }

    public void setNeighbor(Text neighbor) {
        this.neighbor = neighbor;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WordPair)) return false;

        WordPair wordPair = (WordPair) o;

        if (!getWord ().equals ( wordPair.getWord () )) return false;
        return getNeighbor ().equals ( wordPair.getNeighbor () );
    }


    @Override
    public int hashCode() {
        int result = getWord ().hashCode ();
        result = 31 * result + getNeighbor ().hashCode ();
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "word=" + word +
                ", neighbor=" + neighbor +
                '}';
    }
}
