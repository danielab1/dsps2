
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class SortingKey implements WritableComparable<SortingKey> {
    private Text firstTwoWords;
    private Text thirdWord;
    private DoubleWritable prob;

    public SortingKey(){
        this.firstTwoWords = new Text();
        this.thirdWord = new Text();
        this.prob = new DoubleWritable(0);
    }

    public SortingKey(Text firstTwoWords, Text thirdWord, DoubleWritable prob) {
        this.firstTwoWords = new Text(firstTwoWords);
        this.thirdWord = new Text(thirdWord);
        this.prob = new DoubleWritable(prob.get());
    }

    public String toString(){
        return this.firstTwoWords.toString() + " " + this.thirdWord.toString() + " " + this.prob.get();
    }

    public int compareTo(SortingKey other) {
        if(firstTwoWords.toString().compareTo(other.getFirstTwoWords().toString()) > 0)
            return 1;

        if(firstTwoWords.toString().compareTo(other.getFirstTwoWords().toString()) < 0)
            return -1;

        return Double.compare(this.prob.get(), other.getProb().get());

    }

    public void write(DataOutput dataOutput) throws IOException {
        this.prob.write(dataOutput);
        this.firstTwoWords.write(dataOutput);
        this.thirdWord.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.prob.readFields(dataInput);
        this.firstTwoWords.readFields(dataInput);
        this.thirdWord.readFields(dataInput);
    }

    public DoubleWritable getProb() {
        return prob;
    }

    public Text getFirstTwoWords() {
        return firstTwoWords;
    }

    public Text getThirdWord() {
        return thirdWord;
    }

}
