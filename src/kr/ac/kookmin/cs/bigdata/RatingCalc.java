package kr.ac.kookmin.cs.bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class RatingCalc extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new RatingCalc(), args);

		System.exit(res);
	}

	public static class StarRatingCompsiteWritable implements Writable {
		long count = 0;
		long rating = 0;

		public StarRatingCompsiteWritable() {
		}

		public StarRatingCompsiteWritable(long val1, long val2) {
			this.count = val1;
			this.rating = val2;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			count = in.readLong();
			rating = in.readLong();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(count);
			out.writeLong(rating);
		}

		public void merge(StarRatingCompsiteWritable other) {
			this.count += other.count;
			this.rating += other.rating;
		}

		@Override
		public String toString() {
			return this.count + "\t" + this.rating;
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));

		Job job = Job.getInstance(getConf());
		job.setJarByClass(RatingCalc.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setMapperClass(Map.class);
		job.setMapOutputValueClass(StarRatingCompsiteWritable.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

	public static class Map extends
			Mapper<LongWritable, Text, Text, StarRatingCompsiteWritable> {
		private Text bId = new Text();
		// private StarRatingCompsiteWritable starsPair = new StarRatingCompsiteWritable();
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();

			try {
				JSONParser jsonParser = new JSONParser();

				// JSON데이터를 넣어 JSON Object 로 만들어 준다.
				JSONObject jsonObject = (JSONObject) jsonParser.parse(line);

				String businessId = (String) jsonObject.get("business_id");
				long starRating = (Long) jsonObject.get("stars");

				bId.set(businessId);
				context.write(bId, new StarRatingCompsiteWritable((long) 1, starRating));

			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, StarRatingCompsiteWritable, Text, FloatWritable> {

		@Override
		protected void reduce(
				Text bId,
				Iterable<StarRatingCompsiteWritable> sRatings,
				Reducer<Text, StarRatingCompsiteWritable, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			
			long total_count = 0;
			long total_rating = 0;
			
			for (StarRatingCompsiteWritable s : sRatings) {
				total_count += s.count;
				total_rating += s.rating;
			}
			
			float avg_rating = (float) total_rating / (float) total_count;
			
			context.write(bId, new FloatWritable(avg_rating));
		}
	}
}