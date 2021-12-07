import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class WeatherHackerMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> 
{

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 
	{
		String valueString = value.toString();
		String[] SplitString = valueString.split(" ");
		
		String feature = WeatherHacker.current_feature;
		int attribute = WeatherHacker.current_attrib;
		if(feature.equals("")) 
		{
			for(int i = 0; i < SplitString.length - 1; i++)
			{
				//System.out.println(i + "-" + SplitString[i] + "-" + SplitString[SplitString.length - 1]);
				output.collect(new Text(i + "-" + SplitString[i] + "-" + SplitString[SplitString.length - 1]), 
						new IntWritable(1));
			}
			output.collect(new Text("size"), new IntWritable(1));
		}
		else
		{
			if(SplitString[attribute].equals(feature))
			{
				for (int i = 0; i < SplitString.length - 1; i++)
				{
					if(i != attribute)
					{
						output.collect(new Text(i + "-" + SplitString[i] + "-" + SplitString[SplitString.length - 1]), 
								new IntWritable(1));
					}
				}
				output.collect(new Text("size"), new IntWritable(1));
			}
		}
	}
}
