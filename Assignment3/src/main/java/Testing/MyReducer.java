package Testing;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import proto.employee.Employee;

import java.io.IOException;

public class MyReducer extends Reducer<ImmutableBytesWritable, Employee, Text, Text> {

    public MyReducer(){
        System.out.println("reached Reducer");
    }

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Employee> values, Reducer<ImmutableBytesWritable, Employee, Text, Text>.Context context) throws IOException, InterruptedException {
        System.out.println("yooo");
    }
}
