package cn.edu360.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

	
	FlowBean v = new FlowBean();
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context)
			throws IOException, InterruptedException {

		long upFlowSum = 0;
		long dFlowSum = 0;

		Iterator<FlowBean> iterator = values.iterator();
		while (iterator.hasNext()) {
			FlowBean bean = iterator.next();

			upFlowSum += bean.getUpFlow();
			dFlowSum += bean.getdFlow();

		}

		v.set(upFlowSum, dFlowSum);
		context.write(key, v);

	}

}
