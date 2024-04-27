package vaccination.state;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class StateAverageReducer
        extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private final DoubleWritable result = new DoubleWritable();
    private final CassandraWriter cassandraWriter;
    public StateAverageReducer() {
        this.cassandraWriter = new CassandraWriter();
    }

    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        double sum = 0.0;
        int count = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }
        double average = sum / count; // Calculate the average
        result.set(average);
        cassandraWriter.writeVaccinationData(key.toString(), average);
        context.write(key, result);
    }
}
