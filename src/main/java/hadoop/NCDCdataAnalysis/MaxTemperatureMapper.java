package hadoop.NCDCdataAnalysis;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private static final Log LOG = LogFactory.getLog(MaxTemperatureMapper.class);
	
	enum map_counter {		  
		temp_greaterthan_30;
	}
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		int temperature = 0;
		String year = "";
		String record = value.toString();
		String airQuality = record.substring(92,93);
		year = record.substring(15, 19);

		if (record.charAt(87) == '+' || record.charAt(87) == '-') {
			temperature = Integer.parseInt(record.substring(88, 92));
		} else {
			temperature = Integer.parseInt(record.substring(87, 92));
		}
		if(temperature >= 30)
		{	
			LOG.info(temperature);
			context.setStatus("Temperature greater than 30 observed");
			context.getCounter(map_counter.temp_greaterthan_30).increment(1);
		}
		if(9999 != temperature && airQuality.matches("[01459]"))		
		{
			context.write(new Text(year), new IntWritable(temperature));
		}
	}

}
