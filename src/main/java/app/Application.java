package app;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import job1.Map;
import job1.Reduce;
import job2.MapTri;
import job3.CustomPartionerStop;
import job3.GroupComparatorStop;
import job3.Mapdt;
import job3.Reducedt;
import job4.MapWordPerDoc;
import job4.ReduceWordPerDoc;
import job5.CustomPartitioner;
import job5.GroupComparator;
import job5.MapFusion;
import job5.ReduceFusion;
import job6.MapFinal;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.BytesWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Application {

	@SuppressWarnings("deprecation")
	public static void main(String[] args)
			throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {

		Path inputFilePath = new Path(args[0]);
		Path FilePathJob1 = new Path("/result/result_job1");
		Path outFilePathJob1 = new Path("/result/result_job1/part-r-00000");
		Path FilePathJob2 = new Path("/result/result_job2");
		Path outFilePathJob2 = new Path("/result/result_job2/part-r-00000");
		Path FilePathJob3 = new Path("/result/result_job3");
		Path outFilePathJob3 = new Path("/result/result_job3/part-r-00000");
		Path FilePathJob4 = new Path("/result/result_job4");
		Path outFilePathJob4 = new Path("/result/result_job4/part-r-00000");
		Path outputFilePath = new Path(args[1]);

		// Suprime le fichier s'il existe
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.newInstance(config);
		List<Path> listPath = new ArrayList<Path>();
		listPath.add(FilePathJob1);
		listPath.add(outFilePathJob1);
		listPath.add(FilePathJob2);
		listPath.add(outFilePathJob2);
		listPath.add(FilePathJob3);
		listPath.add(outFilePathJob3);
		listPath.add(FilePathJob4);
		listPath.add(outFilePathJob4);
		listPath.add(outputFilePath);
		for (Path path : listPath) {
			if (fs.exists(path)) {
				fs.delete(path, true);
			}
		}

		Job job = null;
		try {
			job = new Job(config);
			job.setJobName("MapReduce");
		} catch (IOException e) {
			e.printStackTrace();
		}

		job.addCacheFile(new Path("/wordstop/stopwords_en.txt").toUri());

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, listPath.get(0));

		job.setOutputKeyClass(CustomKey.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setJarByClass(Application.class);
		job.waitForCompletion(true);
		
		Counters counters = job.getCounters();

		System.out.println("Counters Job1: " +
			      counters.findCounter(COUNTER.JOB1).getValue() + "\n");

		Job jobdt = new Job(config);
		jobdt.setJobName("Tri");

		FileInputFormat.setInputPaths(jobdt, listPath.get(1));
		FileOutputFormat.setOutputPath(jobdt, listPath.get(2));

		jobdt.setOutputKeyClass(CustomKey.class);
		jobdt.setOutputValueClass(CustomValue.class);

		jobdt.setMapOutputValueClass(IntWritable.class);

		jobdt.setMapperClass(Mapdt.class);
		jobdt.setReducerClass(Reducedt.class);

		jobdt.setPartitionerClass(CustomPartionerStop.class);
		jobdt.setGroupingComparatorClass(GroupComparatorStop.class);

		jobdt.setJarByClass(Application.class);
		jobdt.waitForCompletion(true);
		
		Counters countersdt = jobdt.getCounters();

		System.out.println("Counters Job2: " +
			      countersdt.findCounter(COUNTER.JOB2).getValue() + "\n");

		Job job2 = new Job(config);
		job2.setJobName("Tri");

		FileInputFormat.setInputPaths(job2, listPath.get(3));
		FileOutputFormat.setOutputPath(job2, listPath.get(4));

		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);

		job2.setMapperClass(MapTri.class);

		job2.setJarByClass(Application.class);
		// job2.waitForCompletion(true);

		Job job3 = new Job(config);
		job3.setJobName("Calcul des Word Per Doc");

		FileInputFormat.setInputPaths(job3, listPath.get(3));
		FileOutputFormat.setOutputPath(job3, listPath.get(4));

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);

		job3.setMapperClass(MapWordPerDoc.class);
		job3.setReducerClass(ReduceWordPerDoc.class);

		job3.setJarByClass(Application.class);
		// job3.waitForCompletion(true);

		Job job4 = new Job(config);
		job4.setJobName("Calcul du TF-IDF");

		// job4.addCacheFile(new Path(listPath.get(5).toString()).toUri());

		FileInputFormat.setInputPaths(job4, listPath.get(3));
		FileOutputFormat.setOutputPath(job4, listPath.get(4));

		job4.setOutputKeyClass(CustomKey.class);
		job4.setOutputValueClass(CustomValue.class);

		job4.setMapperClass(MapFusion.class);
		job4.setReducerClass(ReduceFusion.class);

		job4.setPartitionerClass(CustomPartitioner.class);
		job4.setGroupingComparatorClass(GroupComparator.class);

		job4.setJarByClass(Application.class);
		job4.waitForCompletion(true);

		Job job5 = new Job(config);
		job5.setJobName("Recherche des 20 mots avec la plus grande ponderation");

		FileInputFormat.setInputPaths(job5, listPath.get(5));
		FileOutputFormat.setOutputPath(job5, listPath.get(8));

		//job5.setPartitionerClass(job6.CustomPartitioner.class);
		//job5.setGroupingComparatorClass(job6.GroupComparator.class);

		job5.setOutputKeyClass(DoubleWritable.class);
		job5.setOutputValueClass(CustomKey.class);
		
		//job5.setMapOutputKeyClass(CustomKey.class);
		//job5.setMapOutputValueClass(DoubleWritable.class);

		//job5.setNumReduceTasks(2);
		//MultipleOutputs.addNamedOutput(job5, "test1", TextOutputFormat.class, DoubleWritable.class, CustomKey.class);
		//MultipleOutputs.addNamedOutput(job5, "test2", TextOutputFormat.class, DoubleWritable.class, CustomKey.class);

		job5.setMapperClass(MapFinal.class);

		job5.setJarByClass(Application.class);
		job5.waitForCompletion(true);

	}

}
