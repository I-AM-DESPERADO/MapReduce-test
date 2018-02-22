package cn.edu360.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * KEYIN, VALUEIN, KEYOUT, VALUEOUT
 * 
 * 
 * 
 * @author Administrator
 *
 */

public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	/**
	 * 框架上如何去调用我们的reduce方法呢？
	 * 框架会将收到的来自map阶段的kv数据，按照相同k进行分组，对每一组数据调用一次reduce方法
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		//遍历出当前这一组数据的所有的value（1），然后进行累加，就能得到当前这一组数据（也就是某一个单词）的总次数
		
		int count = 0;
		for(IntWritable value:values){
			
			count += value.get();
			
		}
		
		
		//输出结果即可，具体也不用我们关注，只要丢给context即可
		context.write(key, new IntWritable(count));
		
		
		
	}
	
	
	

}
