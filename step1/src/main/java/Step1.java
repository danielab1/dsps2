import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step1 {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step1");

        // set main class
        job.setJarByClass(Step1.class);

        // set mapper and reducer
        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(TripleKey.class);
        job.setMapOutputValueClass(TripleValue.class);
        job.setNumReduceTasks(2);
        job.setReducerClass(ReducerClass.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setReducerClass(ReducerClass.class);



        job.setPartitionerClass(PartitionerClass.class);


        // set input
//        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileInputFormat.addInputPath(job,new Path(args[1]));
        FileInputFormat.addInputPath(job,new Path(args[2]));

        // set output
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        // wait for completion and exit
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}
