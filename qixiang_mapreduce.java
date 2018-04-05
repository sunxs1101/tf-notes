//需要三样东西实现：一个map函数、一个reduce函数和一些来运行作业的代码
//map函数的实现，由Mapper接口来实现。继承mapper类，重写map方法。
//气象数据集
import java.io.IOException;
import org.apache.hadoop.io.IntWritabel;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
public class MaxTemperatureMapper extends MapReduceBase
	implements Mapper<LongWritable,Text,Text,IntWritabel>{
		private static final int MISSING = 9999;
		public void map(LongWritable key,Text value,
		OutputCollector<Text,IntWritabel> output, Reporter reporter)
		throws IOException{
			String line = value.toString();
			String year = line.substring(15,19);
			int airTemperature;
			if (line.charAt(87) == '+'){
				airTemperature = Integer.parseInt(line.substring(88,92));
			}
			else{
				airTemperature = Integer.parseInt(line.substring(87,92));
			}
			String quality = line.substring(92,93);
			if (airTemperature != MISSING && quality.matches("[01459]")){
				output.collect(new Text(year),new IntWritabel(airTemperature));
			}
		}
	}
	
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritabel;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
public class MaxTemperatureReducer extends MapReduceBase
	implements Reducer<Text,IntWritabel,Text,IntWritabel>{
		public void reduce(Text key,Iterator<IntWritable> values,
		OutputCollector<Text, IntWritabel> output,Reporter reporter)
		throws IOException{
			int maxValue = Integer.MIN_VALUE;
			while (values.hasNext()){
				maxValue = Math.max(maxValue,values.next().get());
			}
			output.collect(key,new IntWritabel(maxValue));
		}
	}

#气温数据集中找出最高气温
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
public class MaxTemperature{
	public static void main(String[] args) throws IOException{
		if(args.length != 2){
			System.err.println("Usage:MaxTemperature<input path><output path>");
			System.exit(-1);
		}
		JobConf conf = new JobConf(MaxTemperature.class);
		conf.setJobName("Max temperature");
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf,new Path(args[1]));
		conf.setMapperClass(MaxTemperatureMapper.class);
		conf.setReducerClass(MaxTemperatureReducer.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritabel.class);
		JobClient.runJob(conf);
	}
}

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	