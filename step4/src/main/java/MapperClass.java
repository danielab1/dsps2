
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MapperClass extends Mapper<Text, DoubleWritable, SortingKey, Text> {

    @Override
    protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
        String[] splited = key.toString().split("\\s+");
        Text firstTwoWords = new Text(splited[0] + " " + splited[1]);
        Text lastWord = new Text(splited[2]);

        context.write(new SortingKey(firstTwoWords,lastWord,value),new Text(""));
    }
}
