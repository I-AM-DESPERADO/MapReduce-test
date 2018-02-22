package cn.edu360.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * KEYIN, VALUEIN, KEYOUT, VALUEOUT
 * 
 * 
 * KEYIN 是框架传递给我们的业务逻辑方法的输入数据中的key的类型 ，默认情况下，框架传递给我们的map逻辑方法的输入数据的key上框架所读取到的一行数据的起始偏移量
 * VALUEIN  是框架传递给我们的业务逻辑方法的输入数据中的value的类型，默认情况下，框架传递给我们的map逻辑方法的输入数据的value是框架所读取到的一行数据的内容
 * 
 * KEYOUT 是我们自己的逻辑处理结果的数据中的key的类型
 * VALUEOUT 是我们自己的逻辑处理结果的数据中的value的类型
 * 
 * 
 * 
 * 在hadoop中，所有的数据需要经过网络传递，就需要对数据进行序列化
 * Long, String, String, Integer 这些类型可以序列化吗？jdk中的这些数据类型可以序列化：serializable机制，信息冗余严重
 * 所以hadoop专门开发了一套序列化机制writable，可以更精简高效低序列化对象
 * 上述四中基本类型，对应的高效序列化类型：  LongWritable, Text, Text, IntWritable
 * 
 * 
 * @author Administrator
 *
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	/**
	 * 框架在map阶段所调用的用户自定义逻辑方法
	 */
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {

		// 将当前这一行切分单词
		
		String line = value.toString();
		String[] words = line.split(" ");
		
		// 对每一个单词输出<单词，1>
		for (String word : words) {
			// map阶段的输出应该上传递给reduce阶段的程序，但是，并不用我们取关心，我们只要把数据丢给框架提供的context即可
			context.write(new Text(word), new IntWritable(1));
			
		}
		
		
		
		
	}
	
	

}
