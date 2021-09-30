package Utils;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class LoadToHdfs {

    public static void main(String[] args) throws IOException {
        LoadToHdfs obj = new LoadToHdfs();
        obj.loadToHdfs();
    }

    public void loadToHdfs() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        String dest = "Assignments/Assisgnment1/inputs";
        Path destPath = new Path(dest);
        if(!checkIfDirectoryExists(fileSystem, destPath)) {
            System.out.println("directory do not exists");
            loadFromLocalToHdfs(fileSystem, destPath);
        }
    }

    private void loadFromLocalToHdfs(FileSystem fileSystem, Path destPath) throws IOException {
        Path srcPath = new Path("Assignment1input0");
        fileSystem.copyFromLocalFile(srcPath, destPath);
        System.out.println("loaded file to hdfs");
    }

    private boolean checkIfDirectoryExists(FileSystem fileSystem, Path destPath) throws IOException {
        return fileSystem.exists(destPath);
    }
}
