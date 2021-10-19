package Assignment4_1;

import java.io.IOException;

public class Assignment4_1 {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        FilesToHDFS filesToHDFS = new FilesToHDFS();
        filesToHDFS.storeFiles();
        MapDriver mapDriver = new MapDriver();
        mapDriver.runDriver();
    }
}
