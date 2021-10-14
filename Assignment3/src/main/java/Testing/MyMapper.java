package Testing;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import proto.employee.Employee;
import proto.employee.Floor;

import java.io.IOException;
import java.util.UUID;

public class MyMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    public MyMapper(){
        System.out.println("reached mapper");
    }

//    @Override
//    protected void setup(Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
//
//    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(";");
        Employee.Builder emp = Employee.newBuilder();
        emp.setEmployeeId(Integer.parseInt(values[0]))
                .setName(values[1])
                .setBuildingCode(Integer.parseInt(values[2]))
                .setFloorNumber(Floor.forNumber(Integer.parseInt(values[3])))
                .setSalary(Integer.parseInt(values[4]))
                .setDepartment(values[5]);
        Employee em = emp.build();
        System.out.println(em);
        String row = UUID.randomUUID().toString();
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("emp_id"), Bytes.toBytes(emp.getEmployeeId()));
        put.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("name"), Bytes.toBytes(emp.getName()));
        put.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("building_code"), Bytes.toBytes(emp.getBuildingCode()));
        put.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("floor_number"), Bytes.toBytes(emp.getFloorNumberValue()));
        put.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("salary"), Bytes.toBytes(emp.getSalary()));
        put.addColumn(Bytes.toBytes("employee"), Bytes.toBytes("department"), Bytes.toBytes(emp.getDepartment()));
        System.out.println(row);
        context.write(new ImmutableBytesWritable(Bytes.toBytes(row)), put);
    }
}
