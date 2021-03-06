package Assignment4_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

import static Assignment4_1.Constants.*;


public class FilesToHDFS {

    // It Stores employee and building serializedfile to hdfs and creates building and employee table.

    public void storeFiles() throws IOException {
        String inputPathToBuildingSerializedFile = PATH + BUILDING_SERIALIZED_FILE;
        String inputPathToEmployeeSerializedFile = PATH + EMPLOYEE_SERIALIZED_FILE;

        storeSerializedFile(inputPathToEmployeeSerializedFile, EMPLOYEE_HDFS_OUTPUT_PATH);
        storeSerializedFile(inputPathToBuildingSerializedFile, BUILDING_HDFS_OUTPUT_PATH);
        createTableEmployee(EMPLOYEE_TABLE_NAME);
        createTableBuilding(BUILDING_TABLE_NAME);
    }

    public void storeSerializedFile(String local_path, String hdfs_output_path) throws IOException {
        InputStream in = new BufferedInputStream(new FileInputStream(local_path));
        Configuration conf = new Configuration();
        Path output = new Path(hdfs_output_path);
        FileSystem fs = FileSystem.get(URI.create(hdfs_output_path), conf);
        if (fs.exists(output)) {                                      // delete existing directory
            fs.delete(output, true);
        }
        System.out.println("Connecting to -- " + conf.get("fs.defaultFS"));
        OutputStream out = fs.create(new Path(hdfs_output_path));
        IOUtils.copyBytes(in, out, 4096, true);
        System.out.println(" copied to HDFS");
    }

    public void createTableBuilding(String tableNameToCreate) throws IOException {

        Configuration config = HBaseConfiguration.create();
        config.addResource("/opt/homebrew/Cellar/hbase/2.4.6/libexec/conf/hbase-site.xml");
        config.set("hbase.zookeeper.quorum","localhost");
        config.set("hbase.zookeeper.property.client.port","2181");
        config.setInt("timeout", 120000);
        config.set("hbase.master", "localhost:60000");
        Connection connection = ConnectionFactory.createConnection(config);
        Admin hAdmin = connection.getAdmin();
        if (hAdmin.tableExists(TableName.valueOf(tableNameToCreate))) {
            System.out.println(TABLE_ALREADY_EXISTS);
            return;
        }
        TableName tname = TableName.valueOf(tableNameToCreate);
        TableDescriptorBuilder tableDescBuilder = TableDescriptorBuilder.newBuilder(tname);
        ColumnFamilyDescriptorBuilder columnDescBuilderBuilding = ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes(COLUMN_FAMILY_BUILDING));
        tableDescBuilder.setColumnFamily(columnDescBuilderBuilding.build());
        tableDescBuilder.build();
        hAdmin.createTable(tableDescBuilder.build());
        System.out.println(tableNameToCreate + TABLE_CREATED);
    }

    public void createTableEmployee(String tableNameToCreate) throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.addResource("/opt/homebrew/Cellar/hbase/2.4.6/libexec/conf/hbase-site.xml");
        config.set("hbase.zookeeper.quorum","localhost");
        config.set("hbase.zookeeper.property.client.port","2181");
        config.setInt("timeout", 120000);
        config.set("hbase.master", "localhost:60000");
        Connection connection = ConnectionFactory.createConnection(config);
        Admin hAdmin = connection.getAdmin();
        if (hAdmin.tableExists(TableName.valueOf(tableNameToCreate))) {
            System.out.println(TABLE_ALREADY_EXISTS);
            return;
        }
        TableName tname = TableName.valueOf(tableNameToCreate);
        TableDescriptorBuilder tableDescBuilder = TableDescriptorBuilder.newBuilder(tname);
        ColumnFamilyDescriptorBuilder columnDescBuilderEmployee = ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes(COLUMN_FAMILY_EMPLOYEE));
        tableDescBuilder.setColumnFamily(columnDescBuilderEmployee.build());
        tableDescBuilder.build();
        hAdmin.createTable(tableDescBuilder.build());
        System.out.println(tableNameToCreate + TABLE_CREATED);
    }

}
