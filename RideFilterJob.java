import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class RideFilterJob {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Ride Filter");

        job.setJarByClass(RideFilterJob.class);
        job.setMapperClass(RideFilterMapper.class);
        job.setReducerClass(RideFilterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(3); // I don't want too many output files

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Adding "filtered" to the output file name
        FileOutputFormat.setOutPath(job, new Path(args[1] + "_filtered"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
