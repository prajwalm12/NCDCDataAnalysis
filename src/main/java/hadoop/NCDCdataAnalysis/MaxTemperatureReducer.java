package hadoop.NCDCdataAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int maximumTemperatureForYear = 0;
		int temp;
		for (IntWritable value : values) {
			temp = value.get();
			if (temp > maximumTemperatureForYear) {
				maximumTemperatureForYear = temp;
			}

		}
		context.write(key, new IntWritable(maximumTemperatureForYear));
	}
}
