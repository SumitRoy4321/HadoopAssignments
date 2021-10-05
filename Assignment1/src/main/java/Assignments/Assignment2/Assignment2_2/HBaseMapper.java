package Assignments.Assignment2.Assignment2_2;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import java.io.IOException;

public class HBaseMapper extends TableMapper<ImmutableBytesWritable, Put> {

    public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
        // process data for the row from the Result instance.
        System.out.println(value);
        context.write(row, resultToPut(row, value));
    }

    private static Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
        Put put = new Put(key.get());
        for (Cell cell : result.listCells()) {
            put.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell));
            System.out.println("added to duplicate table");
        }
        return put;
    }
}
