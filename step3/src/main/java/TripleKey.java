
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class TripleKey implements WritableComparable<TripleKey> {

    private Text firstWord;
    private Text secondWord;
    private Text thirdWord;

    public TripleKey(Text firstWord, Text secondWord, Text thirdWord) {
        this.firstWord = firstWord;
        this.secondWord = secondWord;
        this.thirdWord = thirdWord;
    }

    public TripleKey(Text firstWord, Text secondWord) {
        this(firstWord, secondWord, new Text("~"));
    }

    public TripleKey(Text firstWord) {
        this(firstWord, new Text("~"), new Text("~"));
    }
    public TripleKey() {
        this(new Text(""), new Text(""), new Text(""));
    }

    public void readFields(DataInput in) throws IOException {
        firstWord.readFields(in);
        secondWord.readFields(in);
        thirdWord.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        firstWord.write(out);
        secondWord.write(out);
        thirdWord.write(out);
    }


    // we want to sort the word so that every <w2,w3> and <w1,w2,w3> will be after <w3,*>
    public int compareTo(TripleKey other) {
        // this <w1,w2,~>
        if (isTilda(this.thirdWord)) {
            if (isTilda(other.thirdWord))
                return this.toString().compareTo(other.toString());
                // other is <w1,w2,w3>
            else if (this.firstWord.toString().equals(other.secondWord.toString()))
                if (this.secondWord.toString().equals(other.thirdWord.toString()))
                    return -1;
                else return this.secondWord.toString().compareTo(other.thirdWord.toString());
            else return this.firstWord.toString().compareTo(other.secondWord.toString());
        } else {// this <w1,w2,w3>
            if (isTilda(other.thirdWord)) {
                // other <w1,w2,~>
                if (this.secondWord.toString().equals(other.firstWord.toString()))
                    if (this.thirdWord.toString().equals(other.secondWord.toString()))
                        return 1;
                    else return this.thirdWord.toString().compareTo(other.secondWord.toString());
                else return this.secondWord.toString().compareTo(other.firstWord.toString());
            }else {
                return (this.secondWord.toString() + this.thirdWord.toString()).compareTo((other.secondWord.toString() + other.thirdWord.toString()));// this <w1,w2,w3> <wx,wy,wz>
            }
        }

    }

    public String getFirstWord() {
        return this.firstWord.toString();
    }

    public String getSecondWord() {
        return this.secondWord.toString();
    }

    public String getThirdWord() {
        return this.thirdWord.toString();
    }

    private boolean isTilda(Text word) {
        return word.toString().equals("~");
    }

    @Override
    public String toString() {
        return this.firstWord.toString() + " " + this.secondWord.toString() + " " + this.thirdWord.toString();
    }
}
