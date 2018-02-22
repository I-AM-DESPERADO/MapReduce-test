package cn.edu360.hdfs;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

/**
 * 访问HDFS的基本代码
 * 
 * 
 * 1、确保你的hdfs工作正常
 * 2、在windows上配置hosts文件，把hdfs集群的主机名和ip地址全部配置进去
 * 3、获取hdfs客户端对象时，使用该方法  fs = FileSystem.get(new URI("hdfs://node-1.edu360.cn:9000"), conf, "root");
 * 
 * @author Administrator
 *
 */
public class HdfsTest {
	FileSystem fs;
	
	/*public static void main(String[] args) throws Exception {
		
		//创建一个配置参数对象，需要告诉客户端对象，我们要访问的文件系统是哪一种，以及访问的地址是什么
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://node-1.edu360.cn:9000");
		
		// 首先需要获取HDFS的java客户端对象
		FileSystem fs = FileSystem.get(conf);
		
		
		//从客户端运行所在机器的本地，上传一个文件到HDFS集群
		fs.copyFromLocalFile(new Path("E:/jdk-8u111-linux-i586.tar.gz"), new Path("/"));
		
		// 操作完成后，关闭客户端连接
		fs.close();
		
	}*/
	
	@Before
	public void init() throws Exception{
		
		//创建一个配置参数对象，需要告诉客户端对象，我们要访问的文件系统是哪一种，以及访问的地址是什么
		Configuration conf = new Configuration();
		//conf.set("fs.defaultFS", "hdfs://node-1.edu360.cn:9000");
		
		// 首先需要获取HDFS的java客户端对象
		//fs = FileSystem.get(conf);
		fs = FileSystem.get(new URI("hdfs://node-1.edu360.cn:9000"), conf, "root");
		
	}
	
	
	
	/**
	 * 上传文件测试
	 * @throws Exception 
	 */
	@Test
	public void testUploadFile() throws Exception{
		
		//创建一个配置参数对象，需要告诉客户端对象，我们要访问的文件系统是哪一种，以及访问的地址是什么
				Configuration conf = new Configuration();
				conf.set("fs.defaultFS", "hdfs://node-1.edu360.cn:9000");
				
				// 首先需要获取HDFS的java客户端对象
				FileSystem fs = FileSystem.get(conf);
				
				
				//从客户端运行所在机器的本地，上传一个文件到HDFS集群
				fs.copyFromLocalFile(new Path("E:/jdk-8u111-linux-i586.tar.gz"), new Path("/"));
				
				// 操作完成后，关闭客户端连接
				fs.close();
		
		
	}
	
	/**
	 * 测试下载文件
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * 
	 * 
	 */
	@Test
	public void testDownloadFile() throws IllegalArgumentException, IOException {
		
		// 参数1：上hdfs上的文件路径    参数2：是本地的文件路径
		fs.copyToLocalFile(false,new Path("/NOTICE.txt"), new Path("e:/"),true);
		//fs.copyToLocalFile(new Path("/NOTICE.txt"), new Path("f:/"));
		
		fs.close();
		
		
		
	}
	
	/**
	 * 删除文件
	 * 注意修改hdfs上的文件时，会存在一个权限的问题
	 * 需要在客户端这边声明自己的用户名为hdfs集群上的用户名：root
	 * 声明的方法：1、在run configuration里面加入一个VM参数：  -DHADOOP_USER_NAME=root
	 * 声明的方法：2、在获取fs对象时，指定自身的用户名： FileSystem.get(new URI("hdfs://node-1.edu360.cn:9000"), conf, "root");
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void testDeleteFile() throws IllegalArgumentException, Exception{
		
		fs.delete(new Path("/NOTICE.txt"), true);
		fs.close();
		
	}
	
	
	/**
	 * 
	 * HDFS不支持文件内容的修改（追加除外）
	 * 可以修改文件名
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * 
	 * 
	 */
	@Test
	public void testRename() throws IllegalArgumentException, IOException{
		fs.rename(new Path("/jdk-8u111-linux-i586.tar.gz"), new Path("/jdk.tar.gz"));
		fs.close();
		
		
	}
	
	
	/**
	 * 查看指定目录下的文件信息
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * @throws FileNotFoundException 
	 */
	@Test
	public void testList() throws FileNotFoundException, IllegalArgumentException, IOException{
		
		FileStatus[] files = fs.listStatus(new Path("/"));
		for (FileStatus file : files) {
			System.out.println(file.getPath());
			System.out.println(file.getBlockSize());
			System.out.println(file.getLen());
			System.out.println("---------------------------");
			
		}
		fs.close();
	}
	
	
	
	/**
	 * 查看指定目录下的文件信息
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * @throws FileNotFoundException 
	 */
	@Test
	public void testListFile() throws FileNotFoundException, IllegalArgumentException, IOException{
		
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		
		while(listFiles.hasNext()){
			LocatedFileStatus file = listFiles.next();
			System.out.println(file.getPath());
			System.out.println(file.getBlockSize());
			System.out.println(file.getLen());
			System.out.println("---------------------------");
		}
		
		fs.close();
	}
	
	
	/**
	 * 读取指定文件的指定偏移量范围的数据
	 * 需要用到IO流来进行操作
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void testRangeRead() throws IllegalArgumentException, IOException{
		FSDataInputStream inputStream = fs.open(new Path("/test.data"));
		//inputStream.seek(10240000);
		FileOutputStream outputStream = new FileOutputStream("f:/test.data.1");
		IOUtils.copyLarge(inputStream, outputStream, 6, 4);
		
		IOUtils.closeQuietly(inputStream);
		IOUtils.closeQuietly(outputStream);
		
	}
	
	
	
	
	

}
