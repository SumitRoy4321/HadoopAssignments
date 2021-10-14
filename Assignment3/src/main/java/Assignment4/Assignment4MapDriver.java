package Assignment4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.Driver;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import proto.employee.Employee;

import java.io.IOException;
import java.net.URI;

public class Assignment4MapDriver {

        public static final String EMPLOYEE_HFILE_OUTPUT_PATH = "hdfs://localhost:9000/employee_hfile/";
//        public static final String BUILDING_HFILE_OUTPUT_PATH = "hdfs://localhost:9000/dataDirectory/building_hfile/";
//        public static final String EMPLOYEE_HDFS_INPUT_PATH = Driver.EMPLOYEE_HDFS_OUTPUT_PATH;
//        public static final String BUILDING_HDFS_INPUT_PATH = Driver.BUILDING_HDFS_OUTPUT_PATH;
        public static final String DEFAULT_FS = "fs.defaultFS";
        public static final String HDFS_INPUT_URL = "hdfs://localhost:9000/";
        private static final String BULK_LOADING_MESSAGE = "Bulk Loading HBase Table::";

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set(DEFAULT_FS, HDFS_INPUT_URL);
//            configuration.set(HDFS_IMPL, org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//            configuration.set(FILE_IMPL, org.apache.hadoop.fs.LocalFileSystem.class.getName());

            Path output = new Path(EMPLOYEE_HFILE_OUTPUT_PATH);
            FileSystem hdfs = FileSystem.get(configuration);

            if (hdfs.exists(output)) {                                      // delete existing directory
                    hdfs.delete(output, true);
            }
            Job job = Job.getInstance(configuration, "MRJob");
            job.setJarByClass(Assignment4MapDriver.class);

            TextInputFormat.addInputPath(job, new Path(HDFS_INPUT_URL+"user/sumitroy/Assignment3"));
            HFileOutputFormat2.setOutputPath(job, new Path(EMPLOYEE_HFILE_OUTPUT_PATH));

            job.setMapperClass(ProtoMapper.class);
            job.setReducerClass(ProtoToHbaseReducer.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Put.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
//            TableMapReduceUtil.initTableReducerJob(
//                    "peoples2",      // output table
//                    null,             // reducer class
//                    job);

            job.setNumReduceTasks(0);
            job.waitForCompletion(true);

    }
}
