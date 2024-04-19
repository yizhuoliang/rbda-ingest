import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistanceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private static final DoubleWritable distance = new DoubleWritable();
    private static final Text outKey = new Text("Distance");

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip header row
        if (key.get() == 0 && value.toString().contains("ride_id")) {
            return;
        }

        String[] fields = value.toString().split(",");
        if (fields.length > 12) {
            double startLat = Double.parseDouble(fields[8]);
            double startLng = Double.parseDouble(fields[9]);
            double endLat = Double.parseDouble(fields[10]);
            double endLng = Double.parseDouble(fields[11]);

            double dist = haversine(startLat, startLng, endLat, endLng);
            distance.set(dist);
            context.write(outKey, distance);
        }
    }

    // Haversine formula to calculate the great-circle distance between two points
    private double haversine(double lat1, double lon1, double lat2, double lon2) {
        final double R = 6371.0; // Earth radius in kilometers
        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }
}
