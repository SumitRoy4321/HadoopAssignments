package Assignment4;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import proto.employee.Employee;

import java.io.IOException;

public class ProtoToHbaseReducer extends Reducer<ImmutableBytesWritable, Employee, Text, IntWritable> {

    public ProtoToHbaseReducer(){
        System.out.println("inside reducer");
    }

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Employee> values, Reducer<ImmutableBytesWritable, Employee, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        for(Employee em : values){
            System.out.println(em);
        }
    }
}
