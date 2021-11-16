package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HDFS_Advanced_Operation {

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

        }
    }


}
