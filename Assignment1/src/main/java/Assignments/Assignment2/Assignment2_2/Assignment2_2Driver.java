package Assignments.Assignment2.Assignment2_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.util.Random;
import java.util.UUID;

public class Assignment2_2Driver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration config = HBaseConfiguration.create();
        Job job = Job.getInstance(config, "MRJob");
        job.setJarByClass(Assignment2_2Driver.class);
        job.setMapperClass(HBaseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        Random rand = new Random();
        String fileNum = UUID.randomUUID().toString();
        FileOutputFormat.setOutputPath(job, new Path("/Users/sumitroy/test/"+fileNum));
        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob(
                "peoples",      // input table
                scan,
                HBaseMapper.class,
                null,
                null,
                job);
        TableMapReduceUtil.initTableReducerJob(
                "peoples2",      // output table
                null,             // reducer class
                job);
        job.setNumReduceTasks(0);
        job.submit();
    }
}
