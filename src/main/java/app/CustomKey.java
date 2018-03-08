package app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CustomKey implements WritableComparable<CustomKey> {

	private Text filename;
	private Text word;
	
	public String toString() {
		return filename.toString() + " " + word.toString();
	}
	
	public CustomKey() {
		filename = new Text();
		word = new Text();
	}

	public void readFields(DataInput arg0) throws IOException {
		word.readFields(arg0);
		filename.readFields(arg0);	
	}

	public void write(DataOutput arg0) throws IOException {
		word.write(arg0);
		filename.write(arg0);
	}

	public int compareTo(CustomKey arg0) {
		int comp = this.word.compareTo(arg0.word);
		if(comp!=0) {
			return comp;
		} else {
			return this.filename.compareTo(arg0.filename);
		}
	}
	
	public void set(String filename, String word) {
		this.word.set(word);
		this.filename.set(filename);
	}

	public CustomKey(Text filename, Text word) {
		super();
		this.filename = filename;
		this.word = word;
	}

	public Text getFilename() {
		return filename;
	}

	public Text getWord() {
		return word;
	}

	@Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomKey customKey = (CustomKey) o;
        return Objects.equals(filename, customKey.filename) &&
                Objects.equals(word, customKey.word);
    }
	
}