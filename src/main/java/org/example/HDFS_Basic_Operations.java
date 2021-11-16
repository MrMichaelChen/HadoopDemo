package org.example;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Hello world!
 * Hadoop distribute file api test
 */

public class HDFS_Basic_Operations {

    FileSystem fs;

    Configuration conf;

    @Before
    public void getset() throws Exception {

        // 方法1：从配置文件加载
        System.setProperty("HADOOP_USER_NAME", "root");
        // 默认加载/src配置文件 core-site.xml  hdfs-site.xml
        conf = new Configuration();
        conf.set("fs.defaultFS", "172.18.8.45:9000");
        fs = FileSystem.get(conf);

        /*
        // 方法2：从方法加载
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://172.18.8.45:9000"), configuration, "root");
        */
    }


    @After
    public void end() throws Exception  {
        fs.close();
    }

    //查看ls、创建目录mkdir、上传put、下载、删除del
    @Test
    public void ls() throws Exception {
        Path path = new Path("/tmp");
        FileStatus[] status = fs.listStatus(path);
        for(FileStatus s : status) {
            System.out.println(s.getPath());
            System.out.println(s.getAccessTime());
            System.out.println(s.getLen());
            System.out.println(s.getBlockSize());
        }
    }

    @Test
    public void mkdir() throws Exception {
        Path path = new Path("/tmp/dir");
        boolean flag = fs.mkdirs(path);
        if(flag) {
            System.out.println("mkdir /tmp/dir success~~");
        }
    }

    @Test
    public void put() throws Exception {
        Path path = new Path("/tmp/dir/1.txt");
        FSDataOutputStream out = fs.create(path);
        IOUtils.copyBytes(new FileInputStream(new File("C:\\Users\\cdx\\Documents\\transwarp\\我的坚果云\\05Coding\\HadoopDemo1\\data\\data.txt")), out, conf);
    }

    @Test
    public void del() throws Exception {
        Path path = new Path("/tmp/dir/1.txt");
        fs.delete(path, true);
    }

     @Test
     public void putsmall() throws Exception {
     //小文件上传
     Path path = new Path("/user/bigfile");
     SequenceFile.Writer write = new SequenceFile.Writer(fs, conf, path, Text.class, Text.class);

     File[] files = new File("data/").listFiles();
         assert files != null;
         for(File f : files) {
     write.append(new Text(f.getName()), new Text(FileUtils.readFileToString(f)));
     }
     write.close();
     }

     @Test
     public void getsmall() throws Exception {
     Path path = new Path("/user/bigfile");
     SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

     Text key = new Text();
     Text val = new Text();

     while(reader.next(key, val)) {
     System.out.println("文件内容如下：");
     System.out.println("文件名称：" + key.toString());
     System.out.println("文件内容：" + val.toString());
     }
     }

    @Test
    public void testCopyFromLocalFile() throws IOException {

    // 上传文件
    fs.copyFromLocalFile(new Path("data/data.txt"), new Path("/user/chen/data.txt"));
    System.out.println("over");

    }


    // 文件通过IO流上传
    @Test
    public void putFileToHDFS() throws IOException {

        // 1 创建输入流
        FileInputStream fis = new FileInputStream(new File("data/data.txt"));

        // 2 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/tmp/data.txt"));

        // 3 流对拷
        IOUtils.copyBytes(fis, fos, conf);

        // 4 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    // 文件通过IO流下载
    @Test
    public void getFileFromHDFS() throws IOException {

        // 1 获取输入流
        FSDataInputStream fis = fs.open(new Path("/tmp/data.txt"));

        // 2 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("data_get/get.txt"));

        // 3 流的对拷
        IOUtils.copyBytes(fis, fos, conf);

        // 4 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    // 文件切块下载
    // 第一块
    @Test
    public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取输入流
        FSDataInputStream fis = fs.open(new Path("/tmp/data.txt"));

        // 2 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("data_get/data1.txt"));

        // 3 流的拷贝
        byte[] buf = new byte[1024];

        for(int i =0 ; i < 1024 * 128; i++){
            fis.read(buf);
            fos.write(buf);
        }

        // 4 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    // 第二块
    @Test
    public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException{

        // 1 打开输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        // 2 定位输入数据位置
        fis.seek(1024*1024*128);

        // 3 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part2"));

        // 4 流的对拷
        IOUtils.copyBytes(fis, fos, conf);

        // 5 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

}
