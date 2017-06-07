package goran.rovkp_lab2_zad2;

import java.io.*;
import org.apache.hadoop.io.*;

/**
 *
 * @author goran
 */
public class Data
        implements WritableComparable<Data> {

    private DoubleWritable total;
    private IntWritable cellX;
    private IntWritable cellY;

    public Data() {
        set(new DoubleWritable(), new IntWritable(), new IntWritable());
     }

    public Data(double first, int second, int third) {
        set(first, second, third);
    }

    public Data(DoubleWritable first, IntWritable second, IntWritable third) {
        set(first, second, third);
    }

    public void set(DoubleWritable first, IntWritable second, IntWritable third) {
        this.total = first;
        this.cellX = second;
        this.cellY = third;
    }

    public void set(double first, int second, int third) {
        this.total = new DoubleWritable(first);
        this.cellX = new IntWritable(second);
        this.cellY = new IntWritable(third);
    }

    public double getTotal() {
        return total.get();
    }

    public int getCellX() {
        return cellX.get();
    }
    
    public int getCellY() {
        return cellY.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        total.write(out);
        cellX.write(out);
        cellY.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        total.readFields(in);
        cellX.readFields(in);
        cellY.readFields(in);
    }

    @Override
    public int hashCode() {
        return total.hashCode() * 163 + cellX.hashCode() + cellY.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Data) {
            Data tp = (Data) o;
            return total.equals(tp.total) && cellX.equals(tp.cellX) && cellY.equals(tp.cellY);
        }
        return false;
    }

    @Override
    public String toString() {
        return total.toString() + "\t" + cellX.toString() + "\t" + cellY.toString();
    }

    @Override
    public int compareTo(Data tp) {
        int cmp = total.compareTo(tp.total);
        if (cmp != 0) {
            return cmp;
        }
        cmp = cellX.compareTo(tp.cellX);
        if (cmp != 0) {
            return cmp;
        }
        return cellX.compareTo(tp.cellY);
    }
}
