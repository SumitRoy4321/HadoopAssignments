package Testing;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.hbase.tool.BulkLoadHFilesTool;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Driver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.fs.tmp.dir", "/users/sumitroy/Desktop/dir1");
        Job job = Job.getInstance(config);
        job.setJobName("MRTest");
        job.setJarByClass(Driver.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);
        job.setMapperClass(MyMapper.class);
        TextInputFormat.setInputPaths(job, new Path("EmployeeData.csv"));
        HFileOutputFormat2.setOutputPath(job, new Path("/users/sumitroy/Desktop/dir"));

        TableName tableName = TableName.valueOf("employees");
        try (Connection conn = ConnectionFactory.createConnection(config);
             Table tablee = conn.getTable(tableName);
             RegionLocator regionLocator = conn.getRegionLocator(tableName)) {
             HFileOutputFormat2.configureIncrementalLoad(job, tablee, regionLocator);
        }


//        Configuration hConfig = HBaseConfiguration.create(config);
//        Connection connection = ConnectionFactory.createConnection(config);
//        Table table = connection.getTable(TableName.valueOf("employees"));
//        HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(table.getName()));


        boolean b = job.waitForCompletion(true);
        System.out.println(b);
        if (job.isSuccessful()) {
            try {
                Configuration hConfig = HBaseConfiguration.create();
                BulkLoadHFiles.create(hConfig).bulkLoad(tableName, new Path("/users/sumitroy/Desktop/dir/employee"));
                System.out.println("successful upload to "+tableName+" table");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
