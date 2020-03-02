
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


class TripleKey implements WritableComparable<TripleKey> {

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

        if ((isDollar(this.firstWord)) && (isDollar(other.firstWord))) {
            return 0;

        }
        // case of c0
        if (isDollar(this.firstWord)) {
            return -1;
        }
        if (isDollar(other.firstWord)) {

            return 1;

        }
        if (this.toString().equals(other.toString())) {
            return 0;
        }


        // case this is < w1 ~ ~ >
        if (isTilda(this.secondWord)) {
            // other is <w ~ ~ >
            if (isTilda(other.secondWord)) {
                return this.firstWord.toString().compareTo(other.firstWord.toString());

            }
            // case other is <w1, w2, ~>
            if (isTilda(other.thirdWord)) {
                //and other w2 == this w1
                if (other.secondWord.toString().equals(this.firstWord.toString())) {
                    return -1;
                }
                return this.firstWord.toString().compareTo(other.secondWord.toString());
            }

            // case other <wx,w2,w3> , and other.w3 = this.w1
            if (other.thirdWord.toString().equals(this.firstWord.toString())) {
                return -1;
            }
            // case other <wx,wy,wz> and other wz ! = this.w1
            return this.firstWord.toString().compareTo(other.thirdWord.toString());
        }
        //case this <w1, w2,~>
        else if (isTilda(this.thirdWord)) {
            // case other <wx,~,~> and wx = w2
            if (isTilda(other.secondWord)) {
                if (other.firstWord.toString().equals(this.secondWord.toString()))
                {
                    return 1;
                }
                return this.secondWord.toString().compareTo(other.firstWord.toString());

            }
            //case other <wx,wy,~>
            if (isTilda(other.thirdWord)) {
                if (other.secondWord.toString().equals(this.secondWord.toString()))
                {
                    return this.firstWord.toString().compareTo(other.firstWord.toString());
                }
                else
                {
                   return this.secondWord.toString().compareTo(other.secondWord.toString());


                }

            }
            //case other <wx,wy,wz> and other.wz == this.w2
            else if (other.thirdWord.toString().equals(this.secondWord.toString()))
            {
                return -1;
            }
            // case other.wz !  = this.w2
            return this.secondWord.toString().compareTo(other.thirdWord.toString());

        }
        // case this <w1,w2,w3>
        else {
            // other is <w1,~,~>
            if (isTilda(other.secondWord)) {
                // other is <w1,~,~> and other.w1 = this.w3
                if (other.firstWord.toString().equals(this.thirdWord.toString()))
                {
                    return 1;
                }
                else{
                   return this.thirdWord.toString().compareTo(other.firstWord.toString());
                }

            }
            // other is <wx,wy,~>
            if (isTilda(other.thirdWord)) {
                // other.w2 == this.w3
                if (this.thirdWord.toString().equals(other.secondWord.toString()))
                {
                    return 1;
                }
                // other.w2!=this.w3
               return this.thirdWord.toString().compareTo(other.secondWord.toString());

            }
            //other is <wx,wy,wz>
            if (this.thirdWord.toString().equals(other.thirdWord.toString())) {
                if (this.secondWord.toString().equals(other.secondWord.toString()))
                {
                    return this.firstWord.toString().compareTo(other.firstWord.toString());

                }
                else{
                    return this.secondWord.toString().compareTo(other.secondWord.toString());

                }
            }
            return this.thirdWord.toString().compareTo(other.thirdWord.toString());


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

    private boolean isDollar(Text word) {

        return word.toString().equals("$$$");
    }

    @Override
    public String toString() {

        return this.firstWord.toString() + " " + this.secondWord.toString() + " " + this.thirdWord.toString();
    }
}
