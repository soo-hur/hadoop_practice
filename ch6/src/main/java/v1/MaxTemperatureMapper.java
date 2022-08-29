package v1;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {
    enum Temperature {
        MALFORMED,
        MISSING
    }
    private NcdcRecordParser parser = new NcdcRecordParser();
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            //String line = value.toString();
        parser.parse(value);
        if (parser.isValidTemperature()) {
            int airTemperature = parser.getAirTemperature();
            context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
        } else if (parser.isMalformedTemperature()) {
            System.err.println("Ignoring possibly corrupt input: " + value);
            context.getCounter(Temperature.MALFORMED).increment(1);
        } else if (parser.isMissingTemperature()) {
            context.getCounter(Temperature.MISSING).increment(1);
        }
        context.getCounter("TemperatureQuality", parser.getQuality()).increment(1);
    }
}
