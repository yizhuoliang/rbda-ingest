import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class RideFilterMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0 && value.toString().contains("ride_id")) {
            // Skip header
            return;
        }
        String[] fields = value.toString().split(",");
        // Check for missing fields
        if (fields.length < 13 || containsEmpty(fields)) {
            return;
        }
        // Filtering by time
        LocalDateTime startedAt = LocalDateTime.parse(fields[2], formatter);
        int hour = startedAt.getHour();
        if ((hour >= 7 && hour < 10) || (hour >= 17 && hour < 20)) {
            context.write(new Text(fields[2]), value);
        }
    }

    private boolean containsEmpty(String[] fields) {
        for (String field : fields) {
            if (field == null || field.isEmpty()) {
                return true;
            }
        }
        return false;
    }
}
