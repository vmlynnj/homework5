import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class Kmer {

	

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.err.println("Usage: Driver <in> <out> <k>");
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(2);
		}

		Configuration conf = new Configuration();
		conf.setInt("k", Integer.parseInt(args[2]));
		Job job = Job.getInstance(conf, "Kmer");

		NLineInputFormat.setNumLinesPerSplit(job, 0);
		job.setJarByClass(Kmer.class);

		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);

		job.setInputFormatClass(KmerInputFormatNLines.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		NLineInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,
				new Path("out" + System.currentTimeMillis()));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
