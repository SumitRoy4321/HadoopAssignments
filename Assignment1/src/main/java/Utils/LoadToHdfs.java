package Utils;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

import static Constants.HadoopConstants.ASSISGNMENT1_INPUTS;


public class LoadToHdfs {
    public static FileSystem fileSystem;

//    public static void main(String[] args) throws IOException {
//        LoadToHdfs obj = new LoadToHdfs();
//        obj.loadToHdfs();
//    }

    public LoadToHdfs(FileSystem fileSystem){
        this.fileSystem = fileSystem;
    }

    public void loadToHdfs(String dest, String src) throws IOException {
        Path destPath = new Path(dest);
        if(!checkIfDirectoryExists(fileSystem, destPath)) {
            System.out.println("Directory do not exists");
            loadFromLocalToHdfs(fileSystem, destPath, src);
        }else{

            System.out.println("Directory already exists");
        }
    }

    private void loadFromLocalToHdfs(FileSystem fileSystem, Path destPath, String src) throws IOException {
        Path srcPath = new Path(src);
        fileSystem.copyFromLocalFile(srcPath, destPath);
        System.out.println("loaded file to hdfs");
    }

    private boolean checkIfDirectoryExists(FileSystem fileSystem, Path destPath) throws IOException {
        return fileSystem.exists(destPath);
    }

}
