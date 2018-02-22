package cn.edu360.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FlowCountDriver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Job job = Job.getInstance(new Configuration());
		
		job.setJarByClass(FlowCountDriver.class);
		
		
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		
		FileInputFormat.setInputPaths(job, "f:/flow/input");
		FileOutputFormat.setOutputPath(job, new Path("f:/flow/output"));
		
		
		
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:1);
		
		
		
		
		
		
	}
	
	

}
