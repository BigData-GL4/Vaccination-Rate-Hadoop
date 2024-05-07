package vaccination.state;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class VaccinationByState {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "vaccination rate by state");
        job.setJarByClass(VaccinationByState.class);
        job.setMapperClass(StateVaccinationMapper.class);
        job.setCombinerClass(StateAverageReducer.class);
        job.setReducerClass(StateAverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputFolder = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputFolder)) {
            fs.delete(outputFolder, true);
            System.out.println("Deleted existing output folder.");
        } else {
            System.out.println("Output folder does not exist.");
        }
        FileOutputFormat.setOutputPath(job, outputFolder);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
