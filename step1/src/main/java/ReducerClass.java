
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<TripleKey, TripleValue, TripleKey, TripleValue> {
    private long wordOccurrences;
    private long C0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        wordOccurrences = 0;
        C0=0;
    }

    @Override


    public void reduce(TripleKey key, Iterable<TripleValue> values, Context context)
            throws IOException, InterruptedException {


        TripleValue tripleValue = new TripleValue();
        for (TripleValue value : values) {
            tripleValue.occurrences = tripleValue.Add(tripleValue.occurrences, value.occurrences);
            tripleValue.N1 = tripleValue.Add(tripleValue.N1, value.N1);
            tripleValue.C1 = tripleValue.Add(tripleValue.C1, value.C1);
        }
        if(key.getFirstWord().equals("$")){
            C0=tripleValue.occurrences.get();
        }
        if (key.getSecondWord().equals("~"))
            wordOccurrences = tripleValue.occurrences.get();

        else if (key.getThirdWord().equals("~")) {
            // <w1,w2,~>
            tripleValue.setC1(new LongWritable(wordOccurrences));
            tripleValue.setC0(new LongWritable(C0));
        }
            // <w1,w2,w3>
        else {
            tripleValue.setN1(new LongWritable(wordOccurrences));
            tripleValue.setC0(new LongWritable(C0));
        }

        if (!(key.getSecondWord().equals("~")) && !(key.getFirstWord().equals("$")))
            context.write(key, tripleValue);

    }

}

