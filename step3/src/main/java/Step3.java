
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step3 {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step3");

        // set main class - this job is only of mappers
        job.setJarByClass(Step3.class);

        // set mapper and reducer
        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(TripleKey.class);
        job.setMapOutputValueClass(TripleValue.class);

        job.setReducerClass(ReducerClass.class);


        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job,  new Path(args[1]));

        // set output
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // wait for completion and exit
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}