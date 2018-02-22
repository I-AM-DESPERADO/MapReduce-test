package cn.edu360.mr;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//mapreduce�������ʹ���û��Զ���������������������ݴ��ݣ�����Ҫ���û����Զ������ͱ���ʵ��hadoop�ṩ�����л�����
//Ҳ����ʵ��Writable �ӿ�


public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

	Text k = new Text();
	FlowBean v = new FlowBean();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			String line = value.toString();

			String[] fields = StringUtils.split(line,"\t");

			String phone = fields[1];
			long upFlow = Long.parseLong(fields[fields.length - 3]);
			long dFlow = Long.parseLong(fields[fields.length - 2]);

			k.set(phone);
			v.set(upFlow,dFlow);

			context.write(k, v);

		} catch (Exception e) {

		}

	}

}