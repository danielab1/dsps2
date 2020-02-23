import com.amazonaws.auth.*;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step1 {

    public static void main(String[] args) throws Exception {
        InstanceProfileCredentialsProvider credentialsProvider = new InstanceProfileCredentialsProvider(false);
        // connect to S3
        AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider).withRegion(Regions.US_WEST_2).build();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step1");

        // set main class
        job.setJarByClass(Step1.class);

        // set mapper and reducer
        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(TripleKey.class);
        job.setMapOutputValueClass(TripleValue.class);

        job.setReducerClass(ReducerClass.class);

        job.setPartitionerClass(PartitionerClass.class);
        // set combiner if local aggregation is on
//        if (args[5].equals("true"))
//            job.setCombinerClass(CombinerClass.class);


        // set input
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileInputFormat.addInputPath(job,new Path(args[1]));
        FileInputFormat.addInputPath(job,new Path(args[2]));

        // set output
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        // wait for completion and exit
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}
