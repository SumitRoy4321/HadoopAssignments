package Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

import static Constants.HadoopConstants.HDFSNAME;
import static Constants.HadoopConstants.HDFSPATH;

public class GetHdfsInstance {

    public static FileSystem instance = null;

    private GetHdfsInstance(){}

    public static FileSystem getInstance() throws IOException {
        if(instance == null){
            Configuration configuration = new Configuration();
            configuration.set(HDFSNAME, HDFSPATH);
            instance = FileSystem.get(configuration);
        }
        return instance;
    }
}
