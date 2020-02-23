import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerClass extends Partitioner<TripleKey,TripleValue> {
    @Override
    public int getPartition(TripleKey tripleKey, TripleValue tripleValue, int numPartitions ) {
        if (tripleKey.getSecondWord().equals("~"))
            return  Math.abs(tripleKey.getFirstWord().hashCode()) % numPartitions;
        else if (tripleKey.getThirdWord().equals("~"))
            return  Math.abs(tripleKey.getSecondWord().hashCode()) % numPartitions;
        else if(!tripleKey.getThirdWord().equals("$"))
            return Math.abs(tripleKey.getThirdWord().hashCode()) % numPartitions;
        else return Integer.parseInt(tripleKey.getSecondWord());
    }
}
