
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MapperClass extends Mapper<Text, Text, SortingKey, Text> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String [] keyWords = key.toString().split(" ");
        DoubleWritable val = new DoubleWritable(Double.parseDouble(value.toString()));
        Text firstTwoWords = new Text(keyWords[0] + " " + keyWords[1]);
        Text lastWord = new Text (keyWords[2]);

        context.write(new SortingKey(firstTwoWords,lastWord,val),new Text(""));
    }
}
