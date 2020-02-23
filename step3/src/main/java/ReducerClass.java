import java.io.IOException;

import org.apache.commons.math3.exception.MathArithmeticException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<TripleKey, TripleValue, Text, DoubleWritable> {
    private double N2;
    private double N1;
    private double N3;
    private double C0;
    private double C1;
    private double C2;

    @Override
    protected void reduce(TripleKey key, Iterable<TripleValue> values, Context context) throws IOException, InterruptedException {


        for (TripleValue value : values) {
            N1 = +value.N1.get();
            N3 = +value.N3.get();
            C1 = +value.N1.get();
            C2 = +value.N1.get();
            C0 = Math.max(value.getC0().get(), C0);
            if (key.getThirdWord().equals("~"))
                N2 = value.N2.get();
        }
        double probability = -1;

        if (!key.getThirdWord().equals("~")) {

            double K2 = (Math.log10(N2 + 1) + 1) / (Math.log10(N2 + 1) + 2);
            double K3 = (Math.log10(N3 + 1) + 1) / (Math.log10(N3 + 1) + 2);
            try{
                probability = K3 * (N3 /C2) + (1-K3) * K2*(N2/C1) + (1-K3)* (1-K2) * (N1/C0);

            }
            catch (MathArithmeticException e){
                System.out.println("divided in zero");
            }

                context.write(new Text(key.toString()), new DoubleWritable(probability));

        }
    }
}
