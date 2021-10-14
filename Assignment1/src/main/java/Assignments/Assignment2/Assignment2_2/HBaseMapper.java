package Assignments.Assignment2.Assignment2_2;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HBaseMapper extends TableMapper<ImmutableBytesWritable, Put>{
//    private String hbaseTable;
//    private String dataSeperator;
//    private String columnFamily1;
//    private String columnFamily2;
//    private ImmutableBytesWritable hbaseTableName;
//
//    public void setup(Context context) {
//        Configuration configuration = context.getConfiguration();
//        hbaseTable = configuration.get("hbase.table.name");
//        dataSeperator = configuration.get("data.seperator");
//        columnFamily1 = configuration.get("COLUMN_FAMILY_1");
//        columnFamily2 = configuration.get("COLUMN_FAMILY_2");
//        hbaseTableName = new ImmutableBytesWritable(Bytes.toBytes(hbaseTable));
//    }

    public void map(ImmutableBytesWritable key, Result result, Context context) {
        try {
//            String[] values = value.toString().split(dataSeperator);
//            String rowKey = values[0];
            Put put = new Put(key.get());
            for (Cell cell : result.listCells()) {
            put.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell));
//            System.out.println("added to duplicate table");
            }
//            String[] values = value.toString().split(dataSeperator);
//            String rowKey = values[0];
//            Put put = new Put(Bytes.toBytes(rowKey));
//            put.addColumn(Bytes.toBytes(columnFamily1), Bytes.toBytes("first_name"), Bytes.toBytes(values[1]));
//            put.addColumn(Bytes.toBytes(columnFamily1), Bytes.toBytes("last_name"), Bytes.toBytes(values[2]));
//            put.addColumn(Bytes.toBytes(columnFamily2), Bytes.toBytes("email"), Bytes.toBytes(values[3]));
//            put.addColumn(Bytes.toBytes(columnFamily2), Bytes.toBytes("city"), Bytes.toBytes(values[4]));
            context.write(key, put);
        } catch(Exception exception) {
            exception.printStackTrace();
        }
    }



//        extends TableMapper<ImmutableBytesWritable, Put> {
//
//    public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
//        // process data for the row from the Result instance.
//        System.out.println(value);
//        context.write(row, resultToPut(row, value));
//    }
//
//    private static Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
//        Put put = new Put(key.get());
//        for (Cell cell : result.listCells()) {
//            put.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell));
//            System.out.println("added to duplicate table");
//        }
//        return put;
//    }
}
