import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<TripleKey, TripleValue, TripleKey, TripleValue> {
    private long C2;

    @Override
    protected void reduce(TripleKey key, Iterable<TripleValue> values, Context context) throws IOException, InterruptedException {

        TripleValue tripleValue = new TripleValue();
        for (TripleValue value : values) {
            tripleValue.N1 = tripleValue.Add(tripleValue.N1, value.N1);
            tripleValue.N2 = tripleValue.Add(tripleValue.N2, value.N2);
            tripleValue.C0 = tripleValue.Add(tripleValue.C0, value.C0);
            tripleValue.C1 = tripleValue.Add(tripleValue.C1, value.C1);
            tripleValue.C2 = tripleValue.Add(tripleValue.C2, value.C2);
        }
        if(key.getThirdWord().equals("~"))
            C2= tripleValue.occurrences.get();
        else
            tripleValue.C2 = new LongWritable(C2);
        context.write(key,tripleValue);

    }
}
