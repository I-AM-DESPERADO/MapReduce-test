package cn.edu360.mr;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//mapreduce框架允许使用用户自定义的数据类型来进行数据传递，但是要求用户的自定义类型必须实现hadoop提供的序列化机制
//也就是实现Writable 接口


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
