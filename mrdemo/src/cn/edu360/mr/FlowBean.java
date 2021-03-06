package cn.edu360.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable{
	
	private long upFlow;
	private long dFlow;
	private long sumFlow;
	
	// 序列化机制要求对象必须有一个空参构造函数
	public FlowBean(){}
	
	public FlowBean(long upFlow, long dFlow) {
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.sumFlow = upFlow+dFlow;
	}
	
	public void set(long upFlow, long dFlow) {
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.sumFlow = upFlow+dFlow;
	}
	
	
	
	public long getUpFlow() {
		return upFlow;
	}
	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}
	public long getdFlow() {
		return dFlow;
	}
	public void setdFlow(long dFlow) {
		this.dFlow = dFlow;
	}
	public long getSumFlow() {
		return sumFlow;
	}
	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	// 反序列化时框架调用的方法
	@Override
	public void readFields(DataInput in) throws IOException {
		 
		upFlow = in.readLong();
		dFlow = in.readLong();
		sumFlow = in.readLong();
		
		
		
	}

	
	// 序列化时框架调用的方法
	@Override
	public void write(DataOutput out) throws IOException {
		 
		out.writeLong(upFlow);
		out.writeLong(dFlow);
		out.writeLong(sumFlow);
		
	}

	@Override
	public String toString() {
		
		return upFlow+"\t"+dFlow+"\t"+sumFlow;
		
	}
	
	
	
	
	

}
