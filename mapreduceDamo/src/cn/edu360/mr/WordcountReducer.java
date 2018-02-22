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
	 * ��������ȥ�������ǵ�reduce�����أ�
	 * ��ܻὫ�յ�������map�׶ε�kv���ݣ�������ͬk���з��飬��ÿһ�����ݵ���һ��reduce����
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		//��������ǰ��һ�����ݵ����е�value��1����Ȼ������ۼӣ����ܵõ���ǰ��һ�����ݣ�Ҳ����ĳһ�����ʣ����ܴ���
		
		int count = 0;
		for(IntWritable value:values){
			
			count += value.get();
			
		}
		
		
		//���������ɣ�����Ҳ�������ǹ�ע��ֻҪ����context����
		context.write(key, new IntWritable(count));
		
		
		
	}
	
	
	

}