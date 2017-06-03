package cloud;
import java.util.*;
import java.io.File;
import java.io.IOException;
import java.security.Timestamp;
import java.text.SimpleDateFormat;
import java.io.IOException; 

import org.apache.hadoop.fs.Path;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.util.*;

import com.google.common.collect.Multiset.Entry; 

public class asd {

	//Mapper class 
	public static class E_EMapper extends MapReduceBase implements 
	Mapper<LongWritable ,/*Input key Type */ 
	Text,                /*Input value Type*/ 
	Text,                /*Output key Type*/ 
	Text>        /*Output value Type*/ 
	{ 

		//Map function 
		public void map(LongWritable key, Text value, 
				OutputCollector<Text, Text> output,   
				Reporter reporter) throws IOException 
		{ 
			String line = value.toString(); 
			String bytes = null;
			String time = null;
			StringTokenizer s = new StringTokenizer(line,","); 
			String year = s.nextToken(); 
			int i = 0;
			int c = s.countTokens();
			while(s.hasMoreTokens())
			{
				if (i == 5){
					bytes=s.nextToken();

				}else if (i == 7 && c == 11){
					time = s.nextToken();
				}else if (i==6 && c == 10){
					time = s.nextToken();
				}else{
					s.nextToken();

				}
				i++;
			} 

			//System.out.println(avgprice);
			// System.out.println(time);
			long timestamp = (long) Double.parseDouble(time);
			//System.out.println(timestamp);
			java.util.Date dt = new Date(timestamp*1000);
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/YYYY");
			//System.out.println(cal.getTimeInMillis());
			output.collect(new Text(year), new Text(simpleDateFormat.format(dt)+","+bytes)); 
		} 
	} 


	//Reducer class 
	public static class E_EReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>
	// Reducer< Text, IntWritable, Text, IntWritable > 
	{  

		//Reduce function 
		public void reduce( Text key, Iterator <Text> values, 
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{ 

			HashMap<String, Long> s = new HashMap<>();
			//int val=Integer.MIN_VALUE; 

			while (values.hasNext()){ 
				String va = values.next().toString();
				String[] o = va.split(",");
				int leng = 0;
				if (o.length % 2 == 0){
					leng = o.length;
				}else{
					leng = o.length-1;
				}
				for (int a = 0; a < leng; a+=2){
					if (s.containsKey(o[a])){
						s.put(o[a], s.get(o[a])+Long.parseLong(o[a+1]));
					}else if(!s.containsKey(o[a])){
						s.put(o[a], Long.parseLong(o[a+1]));
					}
				}

			} 
			StringBuilder string = new StringBuilder();
			int vir = 1;
			int avg = 0;
			for ( java.util.Map.Entry<String, Long> val: s.entrySet()){
				string.append(val.getKey()+ ","+val.getValue());
				avg += val.getValue();
				if ( ! (vir == s.size())){
					string.append(",");
				}

				if ( vir == s.size())
					string.append(", media"+avg/s.size());    
				//System.out.println(vir + "   " +key);
				vir ++;
				

			}
			//System.out.println(string);
			output.collect(key, new Text(string.toString()));

		} 
	}  


	//Main function 
	public static void main(String args[])throws Exception 
	{ 
		JobConf conf = new JobConf(asd.class); 

		conf.setJobName("max_eletricityunits"); 
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(E_EMapper.class); 
		conf.setCombinerClass(E_EReduce.class); 
		conf.setReducerClass(E_EReduce.class); 
		conf.setInputFormat(TextInputFormat.class); 
		conf.setOutputFormat(TextOutputFormat.class); 

		FileInputFormat.setInputPaths(conf, new Path("input/data.csv"));
		try {
			File file = new File("output");
			file.delete();
		}catch(Exception e){

		}

		FileOutputFormat.setOutputPath(conf, new Path("output")); 

		JobClient.runJob(conf); 


	} 
}
