
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
            // other <w1,w2,~>
            if (isTilda(other.thirdWord)) {
                if (this.firstWord.toString().equals(other.firstWord.toString()))
                    return this.secondWord.toString().compareTo(other.secondWord.toString());
                else return this.firstWord.toString().compareTo(other.firstWord.toString());
            }
            // other is <w1, w2, w3>
            if (this.firstWord.toString().equals(other.firstWord.toString())) {
                //this.w2 = other.w2
                if (this.secondWord.toString().equals(other.secondWord.toString()))
                    return -1;
                    // this.w2 != other.w2
                else return this.secondWord.toString().compareTo(other.secondWord.toString());
            }
            // this.w1 != other.w1
            else return this.firstWord.toString().compareTo(other.firstWord.toString());

        }
        // this <wx,wy,wz>
        else {
            // other <w1,w2,~>
            if (isTilda(other.thirdWord)) {
                // this.wx = other.w1
                if (this.firstWord.toString().equals(other.firstWord.toString())) {
                    if (this.secondWord.toString().equals(other.secondWord.toString()))
                        return 1;
                    else return this.secondWord.toString().compareTo(other.secondWord.toString());
                } else return this.firstWord.toString().compareTo(other.firstWord.toString());

            } else return this.toString().compareTo(other.toString());
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
        return this.firstWord + " " + this.secondWord + " " + this.thirdWord;
    }
}
