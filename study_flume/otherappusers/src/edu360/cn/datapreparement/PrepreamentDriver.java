package edu360.cn.datapreparement;

import java.io.IOException;

import javax.xml.soap.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PrepreamentDriver {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		
		Configuration conf = new Configuration();
		
		
		Job job = Job.getInstance(conf, args[0]);
		
		job.setJarByClass(PrepreamentDriver.class);
		
		job.setMapperClass(PreparementMap.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		Path inputPath = new Path(args[1]);
		FileInputFormat.setInputPaths(job, inputPath);
		
		Path outputPath = new Path(args[2]);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setNumReduceTasks(0);
		
		job.waitForCompletion(true);
		
	}

}
