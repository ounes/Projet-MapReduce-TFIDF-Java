package app;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import job1.Map;
import job3.Reducedt;

public class WordCountMapReduceTest {

	MapDriver<Object, Text, CustomKey, IntWritable> mapDriver;
	ReduceDriver<CustomKey, IntWritable, CustomKey, CustomValue> reduceDriver;
	private static final Path placeholderFilePath = new Path("fileName");

	@Before
	public void setUp() {
		Map mapper = new Map();
		Reducedt reducer = new Reducedt();
		mapDriver = MapDriver.newMapDriver(mapper);
		mapDriver.setMapInputPath(placeholderFilePath);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(1), new Text("horse horse zebra"));
		mapDriver.withOutput(new CustomKey(new Text(placeholderFilePath.toString()), new Text("horse")), new IntWritable(1));
		mapDriver.withOutput(new CustomKey(new Text(placeholderFilePath.toString()), new Text("horse")), new IntWritable(1));
		mapDriver.withOutput(new CustomKey(new Text(placeholderFilePath.toString()), new Text("zebra")), new IntWritable(1));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws IOException {
		List<IntWritable> list = new ArrayList<IntWritable>();
		list.add(new IntWritable(2));
		list.add(new IntWritable(3));
		reduceDriver.withInput(new CustomKey(new Text(placeholderFilePath.toString()), new Text("horse")), list);
		reduceDriver.withInput(new CustomKey(new Text(placeholderFilePath.toString()), new Text("zebra")), list);
		reduceDriver.withOutput(new CustomKey(new Text(placeholderFilePath.toString()), new Text("horse")), 
				new CustomValue(new IntWritable(5),new IntWritable(2)));
		reduceDriver.withOutput(new CustomKey(new Text(placeholderFilePath.toString()), new Text("horse")), 
				new CustomValue(new IntWritable(5),new IntWritable(3)));
		reduceDriver.withOutput(new CustomKey(new Text(placeholderFilePath.toString()), new Text("zebra")), 
				new CustomValue(new IntWritable(5),new IntWritable(2)));
		reduceDriver.withOutput(new CustomKey(new Text(placeholderFilePath.toString()), new Text("zebra")), 
				new CustomValue(new IntWritable(5),new IntWritable(3)));
		reduceDriver.runTest();
	}

}
