package Assignments.Assignment2.Assignment2_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import static Assignments.Assignment2.Assignment2_2.Constants.*;

public class Assignment2_2Driver  {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.master","localhost:60000");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.zookeeper.quorum", "localhost");
        config.set(DEFAULT_FS, HDFS_INPUT_URL);
        Path output = new Path(ASSIGNMENT2_OUTPUT_PATH);
        FileSystem hdfs = FileSystem.get(config);
        if (hdfs.exists(output)) {                                      // delete existing directory
            hdfs.delete(output, true);
        }
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        Job job = Job.getInstance(config);
        job.setJobName("MRTableReadWrite");
        job.setJarByClass(Assignment2_2Driver.class);
        TableMapReduceUtil.initTableMapperJob(
                PEOPLE_TABLE,
                scan,
                HBaseMapper.class,
//                ImmutableBytesWritable.class,
//                Put.class,
                null,
                null,
                job
        );
        TableMapReduceUtil.initTableReducerJob(
                PEOPLE_BULK_UPLOAD_TABLE,      // output table
                null,             // reducer class
                job
        );
        job.setNumReduceTasks(0);
        boolean b = job.waitForCompletion(true);
        System.out.println(b);
        if(job.isSuccessful()){
            System.out.println("Successful");
        }
    }
}
