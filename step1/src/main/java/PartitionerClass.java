import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerClass extends Partitioner<TripleKey, TripleValue> {
    @Override
    public int getPartition(TripleKey tripleKey, TripleValue tripleValue, int numPartitions) {
        if (tripleKey.getSecondWord().equals("~")) // <w1,~,~>
            return Math.abs(tripleKey.getFirstWord().hashCode()) % numPartitions;
        else if (tripleKey.getThirdWord().equals("~")) // <w1,w2,~>
            return Math.abs(tripleKey.getSecondWord().hashCode()) % numPartitions;
        else if (tripleKey.getThirdWord().equals("$$$"))
            Integer.parseInt(tripleKey.getSecondWord()); // <$,index,$>
        return Math.abs(tripleKey.getThirdWord().hashCode()) % numPartitions; // <w1.w2.w3>




    }
}