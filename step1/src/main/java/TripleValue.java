
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TripleValue implements Writable {

    public LongWritable occurrences;
    public LongWritable N1;
    public LongWritable N2;
    public LongWritable N3;
    public LongWritable C0;
    public LongWritable C1;
    public LongWritable C2;

    public void setOccurrences(LongWritable occurrences) {
        this.occurrences = occurrences;
    }

    public LongWritable getN1() {
        return N1;
    }

    public void setN1(LongWritable n1) {
        N1 = n1;
    }

    public LongWritable getN2() {
        return N2;
    }

    public void setN2(LongWritable n2) {
        N2 = n2;
    }

    public LongWritable getN3() {
        return N3;
    }

    public void setN3(LongWritable n3) {
        N3 = n3;
    }

    public LongWritable getC0() {
        return C0;
    }

    public void setC0(LongWritable c0) {
        C0 = c0;
    }

    public LongWritable getC1() {
        return C1;
    }

    public void setC1(LongWritable c1) {
        C1 = c1;
    }

    public LongWritable getC2() {
        return C2;
    }

    public void setC2(LongWritable c2) {
        C2 = c2;
    }


    public TripleValue(LongWritable occurrences, LongWritable N1, LongWritable N2, LongWritable N3,
                       LongWritable C0, LongWritable C1, LongWritable C2){
        this.occurrences = occurrences;
        this.N1 = N1;
        this.N2 = N2;
        this.N3 = N3;
        this.C0 = C0;
        this.C1 = C1;
        this.C2 = C2;
    }
    public TripleValue(LongWritable occurrences){
        this(occurrences, new LongWritable(0), new LongWritable(0),
        new LongWritable(0),new LongWritable(0),new LongWritable(0),new LongWritable(0));
    }
    public TripleValue(){
        this( new LongWritable(0), new LongWritable(0), new LongWritable(0),
                new LongWritable(0),new LongWritable(0),new LongWritable(0),new LongWritable(0));
    }



    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.occurrences.write(dataOutput);
        this.N1.write(dataOutput);
        this.N2.write(dataOutput);
        this.N3.write(dataOutput);
        this.C0.write(dataOutput);
        this.C1.write(dataOutput);
        this.C2.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.occurrences.readFields(dataInput);
        this.N1.readFields(dataInput);
        this.N2.readFields(dataInput);
        this.N3.readFields(dataInput);
        this.C0.readFields(dataInput);
        this.C1.readFields(dataInput);
        this.C2.readFields(dataInput);

    }
    public LongWritable Add(LongWritable value1 , LongWritable value2){
        return new LongWritable(value1.get()+ value2.get());
    }

    public String toString(){
        return "occur:"+occurrences.get() +
                " N1: " +N1.get()+ "  N2: " +N2.get() +" N3: " +N3.get() +" C0: " +C0.get()  +" C1: " +C1.get() +"  C2:" +C2.get() +"\n";
    }

}
