package Assignments;

import Constants.HadoopConstants;
import Utils.GetHdfsInstance;
import Utils.LoadToHdfs;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.util.Arrays;

public class Assignment1 {

    public static FileSystem fileSystem;

    public static void main(String[] args) throws IOException {
        fileSystem = GetHdfsInstance.getInstance();
        LoadToHdfs loadInputsToHdfs = new LoadToHdfs(fileSystem);
        loadInputsToHdfs.loadToHdfs();
        Assignment1 assignment1 = new Assignment1();
        assignment1.loadHbaseTable();
    }

    private void loadHbaseTable() throws IOException {
        Path[] filePaths = readDirectoryContents();
        FSDataInputStream inputStream = null;
//        Configuration config = HBaseConfiguration.create();

        for( Path path : filePaths){
//            System.out.println(path);
            inputStream = fileSystem.open(path);
            String out= IOUtils.toString(inputStream, "UTF-8");

            String[] data = out.split(";");
            System.out.println(Arrays.toString(data));

        }
    }

    private Path[] readDirectoryContents() throws IOException {
        FileStatus[] fileStatus = fileSystem.listStatus(
                new Path(HadoopConstants.HDFSPATH+HadoopConstants.INPUT1DIRECTORYPATH));
        Path[] paths = FileUtil.stat2Paths(fileStatus);
        return paths;
    }
}
