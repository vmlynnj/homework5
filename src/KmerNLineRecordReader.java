
import java.io.BufferedReader;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;


public class KmerNLineRecordReader extends RecordReader<LongWritable, Text> {
		private LongWritable key;
		private Text value;
		private FSDataInputStream fsinstream;
		private FileSystem fileSystem;
		private  long splitStart = 0;
		private long splitLength = 0;
		private long bytesRead = 0;
		private BufferedReader reader;
		private FileSplit fileSplit;
		private Configuration conf;
		private final int newLineBytes = ("\n").getBytes().length;


		public String readNLines() throws IOException {
			String Nlines = null;
			String line = null;
			for (int i = 0; (i < 2) && ((line = reader.readLine()) != null); i++) {
				if (Nlines == null) {
					Nlines = line;
				} else {
					Nlines = Nlines.concat(line);
				}
			}
			return Nlines;
		}
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			this.fileSplit = (FileSplit) split;
			this.conf = context.getConfiguration();
			fileSystem = fileSplit.getPath().getFileSystem(conf);
			fsinstream = fileSystem.open(fileSplit.getPah());
			reader = new BufferedReader(new InputStreamReader(fsinstream));
			splitStart = fileSplit.getStart();
			splitLength = fileSplit.getLength();

			if(splitStart == 0){
				splitLength++;
			}
			else {
				splitStart++;
			}
			fsinstream.skip(splitStart);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if(bytesRead >= splitLength){
				return false;
			} else {
				String line;
				if((line = readNLines()) != null){
					bytesRead += (line.getBytes().length);
					value = new Text(line);
					key = new LongWritable(splitStart);
				}
				else {
					return false;
				}
			}
			}
		@Override
		public void close() throws IOException {
			// do nothing
		}

		@Override
		public LongWritable getCurrentKey() {
			return this.key;
		}

		@Override
		public Text getCurrentValue() {
			return this.value;
		}

		@Override
		public float getProgress() throws IOException {
			return true ? 1.0f : 0.0f;
		}
	}