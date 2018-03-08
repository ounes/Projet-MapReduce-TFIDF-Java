package app;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class CustomValue implements WritableComparable<CustomValue> {
	
	private IntWritable wordPerDoc;
	private IntWritable wordCount;
	
	@Override
	public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomValue CustomValue = (CustomValue) o;
        return Objects.equals(wordPerDoc, CustomValue.wordPerDoc) &&
                Objects.equals(wordCount, CustomValue.wordCount);
    }
	
	public CustomValue(IntWritable wordPerDoc, IntWritable wordCount) {
		super();
		this.wordPerDoc = wordPerDoc;
		this.wordCount = wordCount;
	}

	public String toString() {
		return wordCount.toString() + " " + wordPerDoc.toString();
	}

	public CustomValue() {
		wordPerDoc = new IntWritable();
		wordCount = new IntWritable();
	}

	public IntWritable getWordPerDoc() {
		return wordPerDoc;
	}

	public IntWritable getWordCount() {
		return wordCount;
	}

	public void readFields(DataInput arg0) throws IOException {
		wordCount.readFields(arg0);
		wordPerDoc.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		wordCount.write(arg0);
		wordPerDoc.write(arg0);
	}

	public int compareTo(CustomValue arg0) {
		int comp = this.wordCount.compareTo(arg0.wordCount);
		if(comp!=0) {
			return comp;
		} else {
			return this.wordPerDoc.compareTo(arg0.wordPerDoc);
		}
	}
	
	public void set(int wordPerDoc, int wordCount) {
		this.wordCount.set(wordCount);
		this.wordPerDoc.set(wordPerDoc);
	}

}
