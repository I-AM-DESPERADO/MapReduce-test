package cn.edu360.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



/**
 * 描述我们的这个wordcount程序中所用到各类参数、各种组件
 * 然后提交给yarn集群取运行
 * @author Administrator
 *
 */

public class Driver {
	

	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		/**
		 * 告诉框架，我们这个程序中的mapper、reducer各种类所在的jar包文件在哪里
		 */
		job.setJarByClass(Driver.class);
		
		/**
		 * 描述我们这个job所用的mapper累和reducer类的信息
		 */
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		/**
		 * 
		 * 指定框架用来读取数据的工具类，以及用来最终写出结果的工具类
		 */
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		/**
		 * 指定我们的job要处理的数据所在的目录，以及最终输出结果所在的目录
		 * 
		 */
		FileInputFormat.setInputPaths(job, new Path("/wordcount/input/"));
		FileOutputFormat.setOutputPath(job, new Path("/wordcount/output/"));
		
		
		
		// 向yarn集群提交job启动执行
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:1);
		
	}

}
