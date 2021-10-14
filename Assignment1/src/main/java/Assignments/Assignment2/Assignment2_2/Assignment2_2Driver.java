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

public class Assignment2_2Driver  {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration config = HBaseConfiguration.create();
        Job job =Job.getInstance(config);
        job.setJarByClass(Assignment2_2Driver.class);
        job.setJobName("Bulk Loading HBase Table::");
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapperClass(HBaseMapper.class);
//        FileInputFormat.addInputPaths(job, args[0]);
        Connection connection = ConnectionFactory.createConnection(config);
        FileSystem.getLocal(config).delete(new Path("/Users/sumitroy/Desktop/test"), true);
        FileOutputFormat.setOutputPath(job, new Path("/Users/sumitroy/Desktop/test"));
        job.setMapOutputValueClass(Put.class);
//        Table table =
//        HFileOutputFormat2.configureIncrementalLoad(job, new HTable(configuration,TABLE_NAME));
        HFileOutputFormat2.configureIncrementalLoad(job, connection.getTable(TableName.valueOf("peoples")),
                    connection.getRegionLocator(TableName.valueOf("peoples")));
        job.waitForCompletion(true);
//        Scan scan = new Scan();
//        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
//        scan.setCacheBlocks(false);
//        TableMapReduceUtil.initTableMapperJob(
//                "peoples",      // input table
//                scan,
//                HBaseMapper.class,
//                null,
//                null,
//                job);
//        TableMapReduceUtil.initTableReducerJob(
//                "peoples2",      // output table
//                null,             // reducer class
//                job);
//        job.setNumReduceTasks(0);
//        job.submit();


    }

//    private static final String DATA_SEPERATOR = ",";
//    private static final String TABLE_NAME = "peoples";
//    private static final String COLUMN_FAMILY_1="people";
//    private static final String COLUMN_FAMILY_2="contactDetails";
//    /**
//     * HBase bulk import example
//     * Data preparation MapReduce job driver
//     *
//     * args[0]: HDFS input path
//     * args[1]: HDFS output path
//     *
//     */
//    public static void main(String[] args) {
//        try {
//            int response = ToolRunner.run(HBaseConfiguration.create(), new Assignment2_2Driver(), args);
//            if(response == 0) {
//                System.out.println("Job is successfully completed...");
//            } else {
//                System.out.println("Job failed...");
//            }
//        } catch(Exception exception) {
//            exception.printStackTrace();
//        }
//    }
//
//
//    @Override
//    public int run(String[] args) throws Exception {
//        int result=0;
//        String outputPath = args[1];
//        Configuration configuration = getConf();
//        configuration.set("data.seperator", DATA_SEPERATOR);
//        configuration.set("hbase.table.name",TABLE_NAME);
//        configuration.set("COLUMN_FAMILY_1",COLUMN_FAMILY_1);
//        configuration.set("COLUMN_FAMILY_2",COLUMN_FAMILY_2);
//        Job job =Job.getInstance(configuration);
//        job.setJarByClass(Assignment2_2Driver.class);
//        job.setJobName("Bulk Loading HBase Table::"+TABLE_NAME);
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
//        job.setMapperClass(HBaseMapper.class);
//        FileInputFormat.addInputPaths(job, args[0]);
//        Configuration config = HBaseConfiguration.create();
//        Connection connection = ConnectionFactory.createConnection(config);
//        FileSystem.getLocal(getConf()).delete(new Path(outputPath), true);
//        FileOutputFormat.setOutputPath(job, new Path(outputPath));
//        job.setMapOutputValueClass(Put.class);
////        Table table =
////        HFileOutputFormat2.configureIncrementalLoad(job, new HTable(configuration,TABLE_NAME));
//        HFileOutputFormat2.configureIncrementalLoad(job, connection.getTable(TableName.valueOf(TABLE_NAME)),
//                    connection.getRegionLocator(TableName.valueOf(TABLE_NAME)));
//        job.waitForCompletion(true);
//        if (job.isSuccessful()) {
//            HBaseBulkLoad.doBulkLoad(outputPath, TABLE_NAME);
//        } else {
//            result = -1;
//        }
//        return result;
//    }
}
