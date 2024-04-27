package vaccination.state;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StateVaccinationMapper
        extends Mapper<Object, Text, Text, DoubleWritable> {

    private final static DoubleWritable mmr = new DoubleWritable();
    private final Text state = new Text();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");

        String stateName = tokens[1].trim();
        state.set(stateName);

        try {
            double mmrValue = Double.parseDouble(tokens[9].trim());
            if (mmrValue > 0 ) {
                mmr.set(mmrValue);
                context.write(state, mmr);
            }
        } catch (NumberFormatException e) {
            System.err.println("Error parsing mmr value: " + e.getMessage());
        }
    }
}
