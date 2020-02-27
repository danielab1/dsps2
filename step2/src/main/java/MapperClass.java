
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MapperClass extends Mapper<Text, Text, TripleKey, TripleValue> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        String [] valueWords = value.toString().split(" ");
        String [] keyWords = key.toString().split(" ");

        TripleKey key1 = new TripleKey(new Text(keyWords[0]),
                new Text(keyWords[1]) , new Text(keyWords[2]));

        TripleValue val = new TripleValue(new LongWritable(Long.parseLong(valueWords[0])),
                new LongWritable(Long.parseLong(valueWords[1])),new LongWritable(Long.parseLong(valueWords[2])),
                new LongWritable(Long.parseLong(valueWords[3])),new LongWritable(Long.parseLong(valueWords[4])),
                new LongWritable(Long.parseLong(valueWords[5])),new LongWritable(Long.parseLong(valueWords[6])));
            context.write(key1,val);

    }
}