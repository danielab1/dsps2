
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
        System.out.println("this: ----- " + this.toString() + " other is: ---- " + other.toString());

        if ((isDollar(this.firstWord)) && (isDollar(other.firstWord))) {
            int a = 0;
            System.out.println("0 " + a);
            return a;

        }
        // case of c0
        if (isDollar(this.firstWord)) {
            int a = -1;
            System.out.println("1" + a);
            return a;
        }
        if (isDollar(other.firstWord)) {

            int a = 1;
            System.out.println("2" + a);
            return a;
        }
        if (this.toString().equals(other.toString())) {
            int a = 0;
            System.out.println("3" + a);
            return a;
        }


        // case this is < w1 ~ ~ >
        if (isTilda(this.secondWord)) {
            // other is <w ~ ~ >
            if (isTilda(other.secondWord)) {
                int a = this.firstWord.toString().compareTo(other.firstWord.toString());

                System.out.println("4" + a);
                return a;
            }
            // case other is <w1, w2, ~>
            if (isTilda(other.thirdWord)) {
                //and other w2 == this w1
                if (other.secondWord.toString().equals(this.firstWord.toString())) {

                    int a = -1;

                    System.out.println("5" + a);
                    return a;
                }
                int a = this.firstWord.toString().compareTo(other.secondWord.toString());

                System.out.println("6" + a);
                return a;
            }

            // case other <wx,w2,w3> , and other.w3 = this.w1
            if (other.thirdWord.toString().equals(this.firstWord.toString())) {
                int a = -1;
                System.out.println("7" + a);
                return a;
            }
            // case other <wx,wy,wz> and other wz ! = this.w1
            int a = this.firstWord.toString().compareTo(other.thirdWord.toString());
            System.out.println("8" + a);
            return a;
        }
        //case this <w1, w2,~>
        else if (isTilda(this.thirdWord)) {
            // case other <wx,~,~> and wx = w2
            if (isTilda(other.secondWord)) {
                if (other.firstWord.toString().equals(this.secondWord.toString()))
                {
                    int a = 1;
                    System.out.println("8" + a);
                    return a;
                }
                int a = this.secondWord.toString().compareTo(other.firstWord.toString());
                System.out.println("9" + a);
                return a;

            }
            //case other <wx,wy,~>
            if (isTilda(other.thirdWord)) {
                if (other.secondWord.toString().equals(this.secondWord.toString()))
                {
                    int a = this.firstWord.toString().compareTo(other.firstWord.toString());
                    System.out.println("10" + a);
                    return a;
                }
                else
                {
                    int a = this.secondWord.toString().compareTo(other.secondWord.toString());
                    System.out.println("11" + a);
                    return a;

                }

            }
            //case other <wx,wy,wz> and other.wz == this.w2
            else if (other.thirdWord.toString().equals(this.secondWord.toString()))
            {
                int a =-1;
                System.out.println("12" + a);
                return a;
            }
            // case other.wz !  = this.w2
            int a =this.secondWord.toString().compareTo(other.thirdWord.toString());
            System.out.println("13" + a);
            return a;
        }
        // case this <w1,w2,w3>
        else {
            // other is <w1,~,~>
            if (isTilda(other.secondWord)) {
                // other is <w1,~,~> and other.w1 = this.w3
                if (other.firstWord.toString().equals(this.thirdWord.toString()))
                {
                    int a =1;
                    System.out.println("14" + a);
                    return a;
                }
                else{
                    int a = this.thirdWord.toString().compareTo(other.firstWord.toString());
                    System.out.println("15" + a);
                    return a;
                }

            }
            // other is <wx,wy,~>
            if (isTilda(other.thirdWord)) {
                // other.w2 == this.w3
                if (this.thirdWord.toString().equals(other.secondWord.toString()))
                {
                    int a = -1;
                    System.out.println("16" + a);
                    return a;
                }
                // other.w2!=this.w3
                int a = this.thirdWord.toString().compareTo(other.secondWord.toString());
                System.out.println("17" + a);
                return a;
            }
            //other is <wx,wy,wz>
            if (this.thirdWord.toString().equals(other.thirdWord.toString())) {
                if (this.secondWord.toString().equals(other.secondWord.toString()))
                {
                    int a = this.firstWord.toString().compareTo(other.firstWord.toString());
                    System.out.println("18" + a);
                    return a;
                }
                else{
                    int a = this.secondWord.toString().compareTo(other.secondWord.toString());
                    System.out.println("19" + a);
                    return a;
                }
            }
            int a = this.thirdWord.toString().compareTo(other.thirdWord.toString());
            System.out.println("20" + a);
            return a;

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

        return word.toString().equals("$");
    }

    @Override
    public String toString() {

        return this.firstWord.toString() + " " + this.secondWord.toString() + " " + this.thirdWord.toString();
    }
}
