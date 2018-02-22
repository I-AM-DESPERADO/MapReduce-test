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
 * ����HDFS�Ļ�������
 * 
 * 
 * 1��ȷ�����hdfs��������
 * 2����windows������hosts�ļ�����hdfs��Ⱥ����������ip��ַȫ�����ý�ȥ
 * 3����ȡhdfs�ͻ��˶���ʱ��ʹ�ø÷���  fs = FileSystem.get(new URI("hdfs://node-1.edu360.cn:9000"), conf, "root");
 * 
 * @author Administrator
 *
 */
public class HdfsTest {
	FileSystem fs;
	
	/*public static void main(String[] args) throws Exception {
		
		//����һ�����ò���������Ҫ���߿ͻ��˶�������Ҫ���ʵ��ļ�ϵͳ����һ�֣��Լ����ʵĵ�ַ��ʲô
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://node-1.edu360.cn:9000");
		
		// ������Ҫ��ȡHDFS��java�ͻ��˶���
		FileSystem fs = FileSystem.get(conf);
		
		
		//�ӿͻ����������ڻ����ı��أ��ϴ�һ���ļ���HDFS��Ⱥ
		fs.copyFromLocalFile(new Path("E:/jdk-8u111-linux-i586.tar.gz"), new Path("/"));
		
		// ������ɺ󣬹رտͻ�������
		fs.close();
		
	}*/
	
	@Before
	public void init() throws Exception{
		
		//����һ�����ò���������Ҫ���߿ͻ��˶�������Ҫ���ʵ��ļ�ϵͳ����һ�֣��Լ����ʵĵ�ַ��ʲô
		Configuration conf = new Configuration();
		//conf.set("fs.defaultFS", "hdfs://node-1.edu360.cn:9000");
		
		// ������Ҫ��ȡHDFS��java�ͻ��˶���
		//fs = FileSystem.get(conf);
		fs = FileSystem.get(new URI("hdfs://node-1.edu360.cn:9000"), conf, "root");
		
	}
	
	
	
	/**
	 * �ϴ��ļ�����
	 * @throws Exception 
	 */
	@Test
	public void testUploadFile() throws Exception{
		
		//����һ�����ò���������Ҫ���߿ͻ��˶�������Ҫ���ʵ��ļ�ϵͳ����һ�֣��Լ����ʵĵ�ַ��ʲô
				Configuration conf = new Configuration();
				conf.set("fs.defaultFS", "hdfs://node-1.edu360.cn:9000");
				
				// ������Ҫ��ȡHDFS��java�ͻ��˶���
				FileSystem fs = FileSystem.get(conf);
				
				
				//�ӿͻ����������ڻ����ı��أ��ϴ�һ���ļ���HDFS��Ⱥ
				fs.copyFromLocalFile(new Path("E:/jdk-8u111-linux-i586.tar.gz"), new Path("/"));
				
				// ������ɺ󣬹رտͻ�������
				fs.close();
		
		
	}
	
	/**
	 * ���������ļ�
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * 
	 * 
	 */
	@Test
	public void testDownloadFile() throws IllegalArgumentException, IOException {
		
		// ����1����hdfs�ϵ��ļ�·��    ����2���Ǳ��ص��ļ�·��
		fs.copyToLocalFile(false,new Path("/NOTICE.txt"), new Path("e:/"),true);
		//fs.copyToLocalFile(new Path("/NOTICE.txt"), new Path("f:/"));
		
		fs.close();
		
		
		
	}
	
	/**
	 * ɾ���ļ�
	 * ע���޸�hdfs�ϵ��ļ�ʱ�������һ��Ȩ�޵�����
	 * ��Ҫ�ڿͻ�����������Լ����û���Ϊhdfs��Ⱥ�ϵ��û�����root
	 * �����ķ�����1����run configuration�������һ��VM������  -DHADOOP_USER_NAME=root
	 * �����ķ�����2���ڻ�ȡfs����ʱ��ָ���������û����� FileSystem.get(new URI("hdfs://node-1.edu360.cn:9000"), conf, "root");
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
	 * HDFS��֧���ļ����ݵ��޸ģ�׷�ӳ��⣩
	 * �����޸��ļ���
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
	 * �鿴ָ��Ŀ¼�µ��ļ���Ϣ
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
	 * �鿴ָ��Ŀ¼�µ��ļ���Ϣ
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
	 * ��ȡָ���ļ���ָ��ƫ������Χ������
	 * ��Ҫ�õ�IO�������в���
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