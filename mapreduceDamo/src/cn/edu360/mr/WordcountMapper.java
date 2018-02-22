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
 * KEYIN �ǿ�ܴ��ݸ����ǵ�ҵ���߼����������������е�key������ ��Ĭ������£���ܴ��ݸ����ǵ�map�߼��������������ݵ�key�Ͽ������ȡ����һ�����ݵ���ʼƫ����
 * VALUEIN  �ǿ�ܴ��ݸ����ǵ�ҵ���߼����������������е�value�����ͣ�Ĭ������£���ܴ��ݸ����ǵ�map�߼��������������ݵ�value�ǿ������ȡ����һ�����ݵ�����
 * 
 * KEYOUT �������Լ����߼���������������е�key������
 * VALUEOUT �������Լ����߼���������������е�value������
 * 
 * 
 * 
 * ��hadoop�У����е�������Ҫ�������紫�ݣ�����Ҫ�����ݽ������л�
 * Long, String, String, Integer ��Щ���Ϳ������л���jdk�е���Щ�������Ϳ������л���serializable���ƣ���Ϣ��������
 * ����hadoopר�ſ�����һ�����л�����writable�����Ը������Ч�����л�����
 * �������л������ͣ���Ӧ�ĸ�Ч���л����ͣ�  LongWritable, Text, Text, IntWritable
 * 
 * 
 * @author Administrator
 *
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	/**
	 * �����map�׶������õ��û��Զ����߼�����
	 */
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {

		// ����ǰ��һ���зֵ���
		
		String line = value.toString();
		String[] words = line.split(" ");
		
		// ��ÿһ���������<���ʣ�1>
		for (String word : words) {
			// map�׶ε����Ӧ���ϴ��ݸ�reduce�׶εĳ��򣬵��ǣ�����������ȡ���ģ�����ֻҪ�����ݶ�������ṩ��context����
			context.write(new Text(word), new IntWritable(1));
			
		}
		
		
		
		
	}
	
	

}