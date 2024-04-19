import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DistanceReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        long count = 0;

        for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }

        if (count > 0) {
            result.set(sum / count);
            context.write(key, result);
        }
    }
}
