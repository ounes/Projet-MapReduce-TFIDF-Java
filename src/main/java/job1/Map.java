package job1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import app.CustomKey;

public class Map extends Mapper<Object, Text, CustomKey, IntWritable> {

	private final static IntWritable number = new IntWritable(1);
	private CustomKey text = new CustomKey();
	private Set<String> stopWordList = new HashSet<String>();
	private BufferedReader fis;

	@SuppressWarnings("deprecation")
	protected void setup(Mapper<Object, Text, CustomKey, IntWritable>.Context context)
			throws IOException, InterruptedException {
		Path[] toto = context.getLocalCacheFiles();
		if (toto != null && toto.length > 0) {
			fis = new BufferedReader(new FileReader(toto[0].toString()));
			String stopword = null;
			while ((stopword = fis.readLine()) != null) {
				stopWordList.add(stopword);
			}
		}
		super.setup(context);
	}

	public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		StringTokenizer st = new StringTokenizer(line.toString().toLowerCase(),
				" \"\\-;\':!?+יט&()[]אח%\t,.0123456789");
		while (st.hasMoreTokens()) {
			String mot = st.nextToken();
			if (mot.length() > 2 && !stopWordList.contains(mot)) {
				text.set(fileName, mot);
				context.write(text, number);
			}
		}
	}
}
