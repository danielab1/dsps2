
import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;


public class MapperClass extends Mapper<TripleKey, TripleValue, TripleKey, TripleValue> {

    @Override
    protected void map(TripleKey key, TripleValue value, Context context) throws IOException, InterruptedException {
        context.write(key,new TripleValue(value));
    }
}