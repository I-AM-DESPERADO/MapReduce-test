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
 * �������ǵ����wordcount���������õ�����������������
 * Ȼ���ύ��yarn��Ⱥȡ����
 * @author Administrator
 *
 */

public class Driver {
	

	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		/**
		 * ���߿�ܣ�������������е�mapper��reducer���������ڵ�jar���ļ�������
		 */
		job.setJarByClass(Driver.class);
		
		/**
		 * �����������job���õ�mapper�ۺ�reducer�����Ϣ
		 */
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		/**
		 * 
		 * ָ�����������ȡ���ݵĹ����࣬�Լ���������д������Ĺ�����
		 */
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		/**
		 * ָ�����ǵ�jobҪ�������������ڵ�Ŀ¼���Լ��������������ڵ�Ŀ¼
		 * 
		 */
		FileInputFormat.setInputPaths(job, new Path("/wordcount/input/"));
		FileOutputFormat.setOutputPath(job, new Path("/wordcount/output/"));
		
		
		
		// ��yarn��Ⱥ�ύjob����ִ��
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:1);
		
	}

}