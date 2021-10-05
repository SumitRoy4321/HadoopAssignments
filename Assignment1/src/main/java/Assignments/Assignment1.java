package Assignments;

import Constants.HadoopConstants;
import Utils.GetHdfsInstance;
import Utils.LoadToHdfs;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Scanner;

import static Constants.HadoopConstants.ASSISGNMENT1_INPUTS;
import static Constants.HadoopConstants.ASSISGNMENT1_INPUTS_SRC;

public class Assignment1 {

    public static FileSystem fileSystem;

    public static void main(String[] args) throws IOException {
        fileSystem = GetHdfsInstance.getInstance();
        LoadToHdfs loadInputsToHdfs = new LoadToHdfs(fileSystem);
        loadInputsToHdfs.loadToHdfs(ASSISGNMENT1_INPUTS, ASSISGNMENT1_INPUTS_SRC);
        Assignment1 assignment1 = new Assignment1();
        assignment1.loadHbaseTable();
    }

    private void loadHbaseTable() throws IOException {
        Path[] filePaths = readDirectoryContents();
        FSDataInputStream inputStream = null;
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);

        Table table = connection.getTable(TableName.valueOf("peoples"));

        int rowNumber=1;
        int fileCount = 1;
        for( Path path : filePaths){
            System.out.println("#### file count :" + fileCount + " " + path);
            fileCount++;
            inputStream = fileSystem.open(path);
            BufferedReader br = new BufferedReader( new InputStreamReader( inputStream, "UTF-8" ) );
            System.out.println(br.readLine());
            String line = br.readLine();
            while(line != null) {
                String out= br.readLine();
                String[] data = out.split(";");
                System.out.println(" inserting data  : " + Arrays.toString(data) + " with size : " + data.length);
                Put p = new Put(Bytes.toBytes("row" + rowNumber));
                p.addColumn(Bytes.toBytes("people"), Bytes.toBytes("name"), Bytes.toBytes(data[0]));
                p.addColumn(Bytes.toBytes("people"), Bytes.toBytes("age"), Bytes.toBytes(data[1]));
                p.addColumn(Bytes.toBytes("people"), Bytes.toBytes("country"), Bytes.toBytes(data[2]));
                p.addColumn(Bytes.toBytes("people"), Bytes.toBytes("building_code"), Bytes.toBytes(data[3]));
                p.addColumn(Bytes.toBytes("people"), Bytes.toBytes("phone"), Bytes.toBytes(data[4]));
                p.addColumn(Bytes.toBytes("people"), Bytes.toBytes("address"), Bytes.toBytes(data[5]));
                table.put(p);
                System.out.println(rowNumber + "   data inserted");
                rowNumber++;
                line = br.readLine();
            }
        }
        table.close();
        connection.close();
        inputStream.close();
        fileSystem.close();
    }

    private Path[] readDirectoryContents() throws IOException {
        FileStatus[] fileStatus = fileSystem.listStatus(
                new Path(HadoopConstants.HDFSPATH+HadoopConstants.INPUT1DIRECTORYPATH));
        Path[] paths = FileUtil.stat2Paths(fileStatus);
        return paths;
    }
}
