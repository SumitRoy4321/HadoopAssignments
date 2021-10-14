package Assignments.Assignment2.Assignment2_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;

public class HBaseBulkLoad {
    /**
     * doBulkLoad.
     *
     * @param pathToHFile path to hfile
     * @param tableName
     */
        public static void doBulkLoad(String pathToHFile, String tableName) {
        try {
            Configuration configuration = new Configuration();
            configuration.set("mapreduce.child.java.opts", "-Xmx1g");
            HBaseConfiguration.addHbaseResources(configuration);
            LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(configuration);
//            HTable hTable = new HTable(configuration, tableName);
            Table hTable = null;
//            loadFfiles.doBulkLoad();
            System.out.println("Bulk Load Completed..");
        } catch(Exception exception) {
            exception.printStackTrace();
        }
    }
}
