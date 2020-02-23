import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerClass extends Partitioner<TripleKey,TripleValue> {
    @Override
    public int getPartition(TripleKey tripleKey, TripleValue tripleValue, int numPartitions) {
        String str = tripleKey.getFirstWord() + tripleKey.getSecondWord();
        return Math.abs(str.hashCode()) % numPartitions;
    }
}
