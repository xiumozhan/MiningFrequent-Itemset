import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class FrequentItemset_MapReduce {
	
	static double s = 0.0;
	static int total = 0;
	static int partition = 1;
	public static final String STRING_SPLIT = ",";
	static List<String> FirstResult = new ArrayList<String>();
	public static IntWritable one = new IntWritable(1);

	public static boolean contain(String[] src, String[] dest) {
		for (int i = 0; i < dest.length; i++) {
			int j = 0;
			for (; j < src.length; j++) {
				if (src[j].equals(dest[i])) {
					break;
				}
			}
			if (j == src.length) {
				return false;// can not find
			}
		}
		return true;
		
	}

	public static class CandidateItemsetMapper extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, IntWritable> {
		
		@Override
		public void map(LongWritable arg0, Text value, OutputCollector<Text, IntWritable> output, Reporter arg3)
				throws IOException {
			List<String[]> data = null;
			try {
				data = loadChessData(value);
			} catch (Exception e) {
				e.printStackTrace();
			}
			Map<String, Double> result = compute(data, s, null, null);
			for (String key : result.keySet()) {
				output.collect(new Text(key), one);
			}
		}

		public Map<String, Double> compute(List<String[]> data, Double minSupport, Integer maxLoop,
				String[] containSet) {
			if (data == null || data.size() <= 0) {
				return null;
			}
			Map<String, Double> result = new TreeMap<String, Double>();
			Map<String, Double> tempresult = new HashMap<String, Double>();
			String[] itemSet = getDataUnitSet(data);
			int loop = 0;
			// loop1
			Set<String> keys = combine(tempresult.keySet(), itemSet);
			tempresult.clear();
			for (String key : keys) {
				tempresult.put(key, computeSupport(data, key.split(STRING_SPLIT)));
			}
			cut(tempresult, minSupport);
			result.putAll(tempresult);
			loop++;
			String[] strSet = new String[tempresult.size()];
			tempresult.keySet().toArray(strSet);
			while (true) {
				keys = combine(tempresult.keySet(), strSet);
				tempresult.clear();
				for (String key : keys) {
					tempresult.put(key, computeSupport(data, key.split(STRING_SPLIT)));
				}
				cut(tempresult, minSupport);
				strSet = new String[tempresult.size()];
				tempresult.keySet().toArray(strSet);
				result.putAll(tempresult);
				loop++;
				if (tempresult.size() <= 0) {
					break;
				}
				if (maxLoop != null && maxLoop > 0 && loop >= maxLoop) {
					break;
				}
			}
			return result;
		}

		public Double computeSupport(List<String[]> data, String[] subSet) {
			Integer value = 0;
			for (int i = 0; i < data.size(); i++) {
				if (contain(data.get(i), subSet)) {
					value++;
				}
			}
			return value * 1.0 / data.size();
		}

		public String[] getDataUnitSet(List<String[]> data) {
			List<String> uniqueKeys = new ArrayList<String>();
			for (String[] dat : data) {
				for (String da : dat) {
					if (!uniqueKeys.contains(da)) {
						uniqueKeys.add(da);
					}
				}
			}
			// String[] toBeStored = list.toArray(new String[list.size()]);
			String[] result = uniqueKeys.toArray(new String[uniqueKeys.size()]);
			return result;
		}

		public Set<String> combine(Set<String> src, String[] target) {
			Set<String> dest = new TreeSet<String>();
			if (src == null || src.size() <= 0) {
				for (String t : target) {
					dest.add(t.toString());
				}
				return dest;
			}
			for (String s : src) {
				for (String t : target) {
					String[] itemset1 = s.split(STRING_SPLIT);
					String[] itemset2 = t.split(STRING_SPLIT);
					int i = 0;
					for (i = 0; i < itemset1.length - 1 && i < itemset2.length - 1; i++) {
						int a = Integer.parseInt(itemset1[i]);
						int b = Integer.parseInt(itemset2[i]);
						if (a != b)
							break;
						else
							continue;
					}
					int a = Integer.parseInt(itemset1[i]);
					int b = Integer.parseInt(itemset2[i]);
					if (i == itemset2.length - 1 && a != b) {
						String keys = s + STRING_SPLIT + itemset2[i];
						String key[] = keys.split(STRING_SPLIT);
						String Checkkeys = null;
						if (a > b) {
							String temp;
							temp = key[key.length - 1];
							key[key.length - 1] = key[key.length - 2];
							key[key.length - 2] = temp;
							keys = key[0];
							for (int j = 0; j < key.length - 1; j++) {
								keys = keys + STRING_SPLIT + key[j + 1];
							}
						}
						if (key.length > 2) {
							int k = 0;
							for (k = 0; k < key.length - 2; k++) {
								int end1 = keys.indexOf(key[k]);
								int start2 = keys.indexOf(key[k + 1]);
								Checkkeys = keys.substring(0, end1) + keys.substring(start2, keys.length());
								if (!src.contains(Checkkeys))
									break;
								else
									continue;
							}
							if (k == key.length - 2)
								dest.add(keys);
						}
						if (Checkkeys == null) {
							if (!dest.contains(keys)) {
								dest.add(keys);
							}
						}
					}
				}
			}
			return dest;
		}

		public Map<String, Double> cut(Map<String, Double> tempresult, Double minSupport) {
			for (Object key : tempresult.keySet().toArray()) {
				if (minSupport != null && minSupport > 0 && minSupport < 1 && tempresult.get(key) < minSupport) {
					tempresult.remove(key);
				}
			}
			return tempresult;
		}

		public static List<String[]> loadChessData(Text value) throws Exception {
			List<String[]> result = new ArrayList<String[]>();
			StringTokenizer baskets = new StringTokenizer(value.toString(), "\n");
			while (baskets.hasMoreTokens()) {
				String[] items = baskets.nextToken().split(" ");
				result.add(items);
			}
			return result;
		}
		
	}

	public static class CandidateItemsetReducer extends MapReduceBase
			implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(1));
		}
	}

	public static void preprocessingphase1(String[] args) throws Exception {
		String originalfilepath = getLocation(args[0]);
		System.out.println(originalfilepath);
		if (originalfilepath == null)
			return;
		List<String> lines = readFile(originalfilepath);
		if (lines == null)
			return;
		total = lines.size();
		partition = Integer.parseInt(args[1]);
		int m = (int) total / partition;
		double m_d = total * 1.0 / partition;
		if (m_d > m)
			m = m + 1;
		mkdir("input_temp");
		for (int i = 0; i < partition; i++) {
			String newpath = "input_temp/" + i + ".dat";
			String input_temp = "";
			for (int j = 0; j < m && total - i * m - j > 0; j++) {
				input_temp += lines.get(i * m + j) + "\n";
			}
			createFile(newpath, input_temp.getBytes());
		}
	}

	public static void preprocessingphase2() throws Exception {
		List<String> lines = readFile("output_temp/part-00000");
		Iterator<String> itr = lines.iterator();
		while (itr.hasNext()) {
			String basket = (String) itr.next();
			String itemset = basket.substring(0, basket.indexOf("\t"));
			FirstResult.add(itemset);
		}
		System.out.println("Pre processing for phase 2 finished.");
	}

	public static class FrequentItemsetMapper extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String data = value.toString();
			String[] baskets = data.split("\n");
			for (int i = 0; i < FirstResult.size(); i++) {
				int number = 0;
				String[] items = FirstResult.get(i).split(STRING_SPLIT);
				for (int j = 0; j < baskets.length; j++) {
					int k = 0;
					for (k = 0; k < items.length; k++) {
						String[] basketsitemset = baskets[j].split(" ");
						if (contain(basketsitemset, items))
							continue;
						else
							break;
					}
					if (k == items.length) {
						number = number + 1;
					}
				}
				output.collect(new Text(FirstResult.get(i)), new IntWritable(number));
			}
		}
	}

	public static class FrequentItemsetReducer extends MapReduceBase
			implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			if (sum >= s * total)
				output.collect(key, new IntWritable(sum));
		}
	}

	public static List<String> readFile(String filePath) throws IOException {
		Path f = new Path(filePath);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(filePath), conf);
		FSDataInputStream dis = fs.open(f);
		InputStreamReader isr = new InputStreamReader(dis, "utf-8");
		BufferedReader br = new BufferedReader(isr);
		List<String> lines = new ArrayList<String>();
		String str = "";
		while ((str = br.readLine()) != null) {
			lines.add(str);
		}
		br.close();
		isr.close();
		dis.close();
		System.out.println("Original file reading complete.");
		return lines;
	}

	public static String getLocation(String path) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path listf = new Path(path);
		FileStatus stats[] = hdfs.listStatus(listf);
		String FilePath = stats[0].getPath().toString();
		hdfs.close();
		System.out.println("Find input file.");
		return FilePath;
	}

	public static void mkdir(String path) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path srcPath = new Path(path);
		boolean isok = fs.mkdirs(srcPath);
		if (isok) {
			System.out.println("create dir ok.");
		} else {
			System.out.println("create dir failure.");
		}
		fs.close();
	}

	public static void createFile(String dst, byte[] contents) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path dstPath = new Path(dst);
		FSDataOutputStream outputStream = fs.create(dstPath);
		outputStream.write(contents);
		outputStream.close();
		fs.close();
		System.out.println("file " + dst + " create complete.");
	}

	public static void phase1(String[] args) throws Exception {
		s = Double.parseDouble(args[2]);
		JobConf conf = new JobConf(FrequentItemset_MapReduce.class);
		conf.setJobName("Find frequent candidate");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(CandidateItemsetMapper.class);
		conf.setReducerClass(CandidateItemsetReducer.class);
		conf.setInputFormat(WholeFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path("input_temp"));
		FileOutputFormat.setOutputPath(conf, new Path("output_temp"));
		JobClient.runJob(conf);
	}

	// phase 2
	public static void phase2(String[] args) throws Exception {
		JobConf conf = new JobConf(FrequentItemset_MapReduce.class);
		conf.setJobName("Frequent Itemsets Count");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(FrequentItemsetMapper.class);
		conf.setReducerClass(FrequentItemsetReducer.class);
		FileInputFormat.setInputPaths(conf, new Path("input_temp"));
		FileOutputFormat.setOutputPath(conf, new Path("output"));
		JobClient.runJob(conf);
	}

	public static class WholeFileRecordReader implements RecordReader<LongWritable, Text> {
		private FileSplit fileSplit;
		private Configuration conf;
		private boolean processed = false;

		public WholeFileRecordReader(FileSplit fileSplit, Configuration conf) throws IOException {
			this.fileSplit = fileSplit;
			this.conf = conf;
		}

		@Override
		public boolean next(LongWritable key, Text value) throws IOException {
			if (!processed) {
				byte[] contents = new byte[(int) fileSplit.getLength()];
				Path file = fileSplit.getPath();
				String fileName = file.getName();
				FileSystem fs = file.getFileSystem(conf);
				FSDataInputStream in = null;
				try {
					in = fs.open(file);
					IOUtils.readFully(in, contents, 0, contents.length);
					value.set(contents, 0, contents.length);
				} finally {
					IOUtils.closeStream(in);
				}
				processed = true;
				return true;
			}
			return false;
		}

		@Override
		public LongWritable createKey() {
			return new LongWritable();
		}

		@Override
		public Text createValue() {
			return new Text();
		}

		@Override
		public long getPos() throws IOException {
			return processed ? fileSplit.getLength() : 0;
		}

		@Override
		public float getProgress() throws IOException {
			return processed ? 1.0f : 0.0f;
		}

		@Override
		public void close() throws IOException {
			// do nothing
		}
	}

	public static class WholeFileInputFormat extends FileInputFormat<LongWritable, Text> {
		@Override
		protected boolean isSplitable(FileSystem fs, Path filename) {
			return false;
		}

		@Override
		public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
				throws IOException {
			return new WholeFileRecordReader((FileSplit) split, job);
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("The number of arguments is less than three.");
			return;
		}
		preprocessingphase1(args);
		phase1(args);
		preprocessingphase2();
		phase2(args);
		List<String> lines = readFile("output/part-00000");
		Iterator<String> itr = lines.iterator();
		File filename = new File("/home/hadoop/Desktop/result.txt");
		filename.createNewFile();
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(filename));
			String firstline = Integer.toString(lines.size()) + "\n";
			out.write(firstline);
			while (itr.hasNext()) {
				String basket = (String) itr.next();
				String itemset = basket.substring(0, basket.indexOf("\t"));
				String number = basket.substring(basket.indexOf("\t") + 1, basket.length());
				out.write(itemset + "(" + number + ")" + "\n");
			}
			out.flush();
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
