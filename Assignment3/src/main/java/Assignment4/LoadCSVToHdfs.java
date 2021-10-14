package Assignment4;

import Utils.GetHdfsInstance;
import Utils.LoadToHdfs;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

import static Assignment4.Assignment4MapDriver.HDFS_INPUT_URL;
import static Constants.HadoopConstants.ASSISGNMENT1_INPUTS;
import static Constants.HadoopConstants.ASSISGNMENT1_INPUTS_SRC;

public class LoadCSVToHdfs {

    public static FileSystem fileSystem;

    public static void main(String[] args) throws IOException {
        fileSystem = GetHdfsInstance.getInstance();
        LoadToHdfs loadInputsToHdfs = new Utils.LoadToHdfs(fileSystem);
        loadInputsToHdfs.copyToHdfs( "EmployeeData.csv", HDFS_INPUT_URL);
    }
}
