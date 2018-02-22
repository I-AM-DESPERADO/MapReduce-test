package edu360.cn.datapreparement;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class PreparementMap extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		
	}
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String str = value.toString();
		
		JSONObject  jo = (JSONObject) JSON.parse(str);
		
		JSONObject joHeader = (JSONObject) jo.get("header");
		
//		JSONObject joEvents = (JSONObject) jo.get("events");
		
	    Object joEvents =  jo.get("events");
	
		
		String sdk_ver_tmp =  " ";
		
		String strSdkVersion = joHeader.getString("sdk_ver");
		
		if(null == strSdkVersion || "".equals(strSdkVersion.trim())){
			return ;
		}
        String strCommitTime = joHeader.getString("commit_time");
		
		if(null == strCommitTime || "".equals(strCommitTime.trim())){
			return ;
		} 
		
		
        String strOsName = joHeader.getString("os_name");
		
		if(null == strOsName || "".equals(strOsName.trim())){
			return ;
		}
		 String strMAC = joHeader.getString("mac");
			
		

		 String strAndroidID = joHeader.getString("android_id");
			

		 String strIMEI = joHeader.getString("imei");
		 
		
		String strUserID = "";
		
		if(("ios").equals(strOsName.trim()) || ("mac").equals(strOsName.trim())){

			strUserID = strMAC;
			
		}else if(("android").equals(strOsName.trim())){

			if(null != strAndroidID && !"".equals(strAndroidID.trim())){
				strUserID = strAndroidID;
			}else if(null != strIMEI && !"".equals(strIMEI.trim())){
				strUserID = strIMEI;
			}else if ( null != strMAC && !"".equals(strMAC.trim())){
				strUserID = strMAC;
			}
			
		}else{

		}

		if("".equals(strUserID)){
			return;
		}

		
		joHeader.put("user_id", strUserID);
		

		JSONObject joNew = new JSONObject();
		joNew.put("header", joHeader);
		joNew.put("events", joEvents);
		
		context.write(new Text(joNew.toJSONString()), NullWritable.get());
//		context.write(new Text(joNew.toJSONString()), new Text());
		
		
	}
	
	

	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

}
