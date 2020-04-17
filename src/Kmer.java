package default;

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
