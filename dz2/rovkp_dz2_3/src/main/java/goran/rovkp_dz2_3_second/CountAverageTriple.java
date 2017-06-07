package goran.rovkp_dz2_3_second;

import java.io.*;
import org.apache.hadoop.io.*;

/**
 *
 * @author goran
 */
public class CountAverageTriple
        implements WritableComparable<CountAverageTriple> {

    private DoubleWritable total;
    private DoubleWritable min;
    private DoubleWritable max;

    public CountAverageTriple() {
        set(new DoubleWritable(), new DoubleWritable(), new DoubleWritable());
    }

    public CountAverageTriple(double first, double second, double third) {
        set(first, second, third);
    }

    public CountAverageTriple(DoubleWritable first, DoubleWritable second, DoubleWritable third) {
        set(first, second, third);
    }

    public void set(DoubleWritable first, DoubleWritable second, DoubleWritable third) {
        this.total = first;
        this.min = second;
        this.max = third;
    }

    public void set(double first, double second, double third) {
        this.total = new DoubleWritable(first);
        this.min = new DoubleWritable(second);
        this.max = new DoubleWritable(third);
    }

    public double getTotal() {
        return total.get();
    }

    public double getMin() {
        return min.get();
    }
    
    public double getMax() {
        return max.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        total.write(out);
        min.write(out);
        max.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        total.readFields(in);
        min.readFields(in);
        max.readFields(in);
    }

    @Override
    public int hashCode() {
        return total.hashCode() * 163 + min.hashCode() + max.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof CountAverageTriple) {
            CountAverageTriple tp = (CountAverageTriple) o;
            return total.equals(tp.total) && min.equals(tp.min) && max.equals(tp.max);
        }
        return false;
    }

    @Override
    public String toString() {
        return total.toString() + "\t" + min.toString() + "\t" + max.toString();
    }

    @Override
    public int compareTo(CountAverageTriple tp) {
        int cmp = total.compareTo(tp.total);
        if (cmp != 0) {
            return cmp;
        }
        cmp = min.compareTo(tp.min);
        if (cmp != 0) {
            return cmp;
        }
        return max.compareTo(tp.max);
    }
}
