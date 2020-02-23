import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerClass extends Partitioner<TripleKey,TripleValue> {
    @Override
    public int getPartition(TripleKey tripleKey, TripleValue tripleValue, int numPartitions) {
        String str;
        if(tripleKey.getThirdWord().equals("~")){
            str = tripleKey.getFirstWord() + tripleKey.getSecondWord();
        }
        else {
            str = tripleKey.getSecondWord() + tripleKey.getThirdWord();
        }

        return Math.abs(str.hashCode()) % numPartitions;
    }
}
