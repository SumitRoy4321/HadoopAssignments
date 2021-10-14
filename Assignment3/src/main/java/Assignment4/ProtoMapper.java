package Assignment4;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Mapper;
import proto.employee.Employee;
import proto.employee.Floor;

import java.io.IOException;

public class ProtoMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Employee> {

    ImmutableBytesWritable TABLE_NAME_TO_INSERT = new ImmutableBytesWritable(Bytes.toBytes("employees"));
    IntWritable INT = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, ImmutableBytesWritable, Employee>.Context context) throws IOException, InterruptedException {
//        System.out.println(value);
        String[] values = value.toString().split(";");
        Employee.Builder emp = Employee.newBuilder();
        emp.setEmployeeId(Integer.parseInt(values[0]))
                .setName(values[1])
                .setBuildingCode(Integer.parseInt(values[2]))
                .setFloorNumber(Floor.forNumber(Integer.parseInt(values[3])))
                .setSalary(Integer.parseInt(values[4]))
                .setDepartment(values[5]);
        Employee em = emp.build();
//        System.out.println(em);
//        context.write(TABLE_NAME_TO_INSERT, em);
        System.out.println("wrote to context " + em.getEmployeeId());
        try {
            System.out.println(context.getReducerClass());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
