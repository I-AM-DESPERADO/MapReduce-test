package cn.edu360.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class IndexCreateStepTwo {
	
	public static class IndexCreateStepTwoMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			
			String[] fields = line.split("-");
			
			context.write(new Text(fields[0]), new Text(fields[1]));
			
			
		}
		
		
		
	}
	public static class IndexCreateStepTwoReducer extends Reducer<Text, Text, Text, Text>{
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {

			StringBuffer sb = new StringBuffer();
			
			for(Text value:values){
				
				sb.append(value).append("\t");
			}
			
			context.write(key, new Text(sb.toString()));
			
		
		}
		
		
	}
	
	public static void main(String[] args) throws Exception {

		Job job = Job.getInstance(new Configuration());
		
		job.setJarByClass(IndexCreateStepTwo.class);
		
		
		job.setMapperClass(IndexCreateStepTwoMapper.class);
		job.setReducerClass(IndexCreateStepTwoReducer.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		
		FileInputFormat.setInputPaths(job, "f:/index/output-1");
		FileOutputFormat.setOutputPath(job, new Path("f:/index/output-2"));
		
		
		
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:1);
	
		
		
	
	}
	
	
	
	

}
