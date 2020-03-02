import org.apache.commons.text.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Arrays;

public class MapperClass extends Mapper<LongWritable, Text, TripleKey, TripleValue> {

    private static long C0;
    private  static final int NUM_OF_REDUCERS = 20;

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        C0 = 0;
    }
        @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
        // value: n-gram \t year \t occurrences \t volume_count \n
        // tokenize the value using tab delimiter

        String[] split_line = value.toString().split("\t");



        // parse the n-gram
        String ngram = split_line[0];

            if (!onlyLettersAndSpace(ngram)) {
                return;
            }

        // parse the occurrences

        LongWritable occurrences = new LongWritable(Long.parseLong(split_line[2]));

        // split n-gram to one/two words/three words
        String[] ngramArray = ngram.split(" ");

        if (ngramArray.length == 1) { // 1-gram
                context.write(new TripleKey(new Text(ngramArray[0]), new Text("~"), new Text("~")),
                        new TripleValue(occurrences));
                C0 = C0 + occurrences.get();
            }
     else if (ngramArray.length==2) { // 2-gram
            context.write(new TripleKey(new Text(ngramArray[0]),new Text(ngramArray[1]),new Text("~")),
                    new TripleValue(occurrences));
        }

     else { //3-gram
            context.write(new TripleKey(new Text(ngramArray[0]),new Text(ngramArray[1]),new Text(ngramArray[2])),
                    new TripleValue(occurrences));
        }



        }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(int i=0; i< NUM_OF_REDUCERS ; i++) {
            context.write(new TripleKey(new Text("$$$"), new Text(String.valueOf(i)), new Text("$$$")),
                    new TripleValue(new LongWritable(C0)));
        }
   }
    private static boolean isLetterOrSpace(char c) {
        return ((c >= (char)1488 && c <= (char)1514) || c == ' ');
    }

    private static boolean onlyLettersAndSpace(String string) {
        for (int i=0; i<string.length(); i++) {
            if (!isLetterOrSpace(string.charAt(i))) {
                return false;
            }
        }
        return true;
    }


}
