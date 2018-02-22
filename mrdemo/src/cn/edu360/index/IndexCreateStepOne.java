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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.edu360.friends.CommonFriendsStepOne;
import cn.edu360.friends.CommonFriendsStepOne.CommonFriendsStepOneMapper;
import cn.edu360.friends.CommonFriendsStepOne.CommonFriendsStepOneReducer;

public class IndexCreateStepOne {
	
	public static class IndexCreateStepOneMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {

			// map方法会被众多maptask调用，而每一个maptask处理的文件及文件内容的范围皆不同，那么，我们在写map方法逻辑的时候，是没法知道当前处理的行属于哪一个文件的
			// 好在，maptask在调用map方法时，会将它所处理的任务切片的信息通过context传递进来，那么，我们就可以通过切片信息获取到当前正在处理的数据所属的文件信息
			
			
			FileSplit split = (FileSplit) context.getInputSplit();
			String fileName = split.getPath().getName();
			
			
			String line = value.toString();
			String[] words = line.split(" ");
			
			for (String word : words) {
				
				context.write(new Text(word+"-"+fileName), new IntWritable(1));
				
			}
			
			
		}
		
		
		
	}
	public static class IndexCreateStepOneReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {

			int count = 0;
			for(IntWritable value:values){
				
				count += value.get();
			}
			
			context.write(key, new IntWritable(count));
			
		}
		
		
	}
	
	public static void main(String[] args) throws Exception {

		Job job = Job.getInstance(new Configuration());
		
		job.setJarByClass(IndexCreateStepOne.class);
		
		
		job.setMapperClass(IndexCreateStepOneMapper.class);
		job.setReducerClass(IndexCreateStepOneReducer.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		
		FileInputFormat.setInputPaths(job, "f:/index/input");
		FileOutputFormat.setOutputPath(job, new Path("f:/index/output-1"));
		
		
		
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:1);
	
		
		
	
	}
	
	
	
	

}
