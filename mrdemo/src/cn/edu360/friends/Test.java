package cn.edu360.friends;

import java.util.ArrayList;

import org.apache.hadoop.io.Text;

public class Test {
	
	public static void main(String[] args) {
		
		ArrayList<Text> list = new ArrayList<Text>();
		
		
		Text p1 = new Text("����");
		
		list.add(p1);
		
		Text p2 = new Text("����");
		
		list.add(p2);
		
		Text p3 = new Text("����");
		
		list.add(p3);
		
		
		System.out.println(list.size());
		
		
	}
	
	

}