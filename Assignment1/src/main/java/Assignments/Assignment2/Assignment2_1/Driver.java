package Assignments.Assignment2.Assignment2_1;

import Utils.GetHdfsInstance;
import Utils.LoadToHdfs;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

import static Constants.HadoopConstants.*;

public class Driver {

    public static FileSystem fileSystem;

    public static void main(String[] args) throws IOException {
        fileSystem = GetHdfsInstance.getInstance();
        LoadToHdfs loadInputsToHdfs = new LoadToHdfs(fileSystem);
        loadInputsToHdfs.loadToHdfs(ASSISGNMENT2_INPUTS, ASSISGNMENT2_INPUTS_SRC);
        wordCount();
    }

    private static void wordCount() throws IOException {
        JobConf conf = new JobConf(Driver.class);
        conf.setJobName("WordCount");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(WordCountMapper.class);
        conf.setCombinerClass(WordCountReducer.class);
        conf.setReducerClass(WordCountReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
//        conf.set
        FileInputFormat.addInputPath(conf, new Path(HDFSPATH+INPUT2PATH));
//        FileInputFormat.setInputPaths(conf, fileSystem.getLinkTarget(new Path(INPUT2PATH)));
        FileOutputFormat.setOutputPath(conf,new Path(HDFSPATH+OUTPUT2PATH));
        JobClient.runJob(conf);
    }
}
