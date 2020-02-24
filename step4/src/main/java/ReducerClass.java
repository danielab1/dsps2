
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerClass extends Reducer<SortingKey, Text, SortingKey, Text> {

    @Override
    protected void reduce(SortingKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(key,new Text(""));


    }

}
