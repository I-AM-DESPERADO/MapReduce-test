package cn.edu360.friends;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.edu360.mr.FlowBean;
import cn.edu360.mr.FlowCountDriver;
import cn.edu360.mr.FlowCountMapper;
import cn.edu360.mr.FlowCountReducer;

public class CommonFriendsStepOne {
	
	
	public static class CommonFriendsStepOneMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {

			// A:B,C,D,F,E,O
			String line = value.toString();
			
			String[] userAndFriends = line.split(":");
			
			String user = userAndFriends[0];
			String[] friends = userAndFriends[1].split(",");
			
			// 发出  k：好友， v:user
			for (String friend : friends) {
				
				context.write(new Text(friend), new Text(user));
				
			}
			
			
			
		}
		
		
		
	}
	
	
	
	
	public static class CommonFriendsStepOneReducer extends Reducer<Text, Text,Text, Text>{
		
		@Override
		protected void reduce(Text friend, Iterable<Text> users,Context context)
				throws IOException, InterruptedException {

			// 好友：C values：所有拥有这个好友的用户：A,B,E
			  
			// 把所有的用户从迭代器中取出来，暂存到一个list中
			ArrayList<Text> usersList = new ArrayList<Text>();
			for(Text user:users){
				Text userCopy = new Text(user);
				usersList.add(userCopy);
				
			}
			
			
			
			
			Collections.sort(usersList);
			
			
			// 配对输出
			for(int i=0;i<usersList.size()-1;i++){
				
				for(int j=i+1;j<usersList.size();j++){
					
					context.write(new Text(usersList.get(i)+"-"+usersList.get(j)), friend);
					
				}
				
				
				
			}
			
			
			
			
			
		}
		
		
		
	}
	
	
	
	
	public static void main(String[] args) throws Exception {
		

		
		Job job = Job.getInstance(new Configuration());
		
		job.setJarByClass(CommonFriendsStepOne.class);
		
		
		job.setMapperClass(CommonFriendsStepOneMapper.class);
		job.setReducerClass(CommonFriendsStepOneReducer.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		
		FileInputFormat.setInputPaths(job, "f:/friend/input");
		FileOutputFormat.setOutputPath(job, new Path("f:/friend/output-1"));
		
		
		
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:1);
	
		
		
	}
	
	

}
