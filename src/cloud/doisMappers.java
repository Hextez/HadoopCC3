package cloud;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;

import cloud.asd.E_EMapper;
import cloud.asd.E_EReduce;

public class doisMappers {



	//Mapper class 
	public static class E_EMapperRECV extends MapReduceBase implements 
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
			StringTokenizer s = new StringTokenizer(line,","); 
			String ip_source = s.nextToken(); 
			String ip_dest = s.nextToken();
			int i = 0;
			int c = s.countTokens();
			while(s.hasMoreTokens() && i < 5)
			{
				if (i == 4){
					bytes=s.nextToken();

				}
				s.nextToken();
				i++;
			} 

			//System.out.println(avgprice);
			// System.out.println(time);
			output.collect(new Text(ip_source+":"+ip_dest), new Text("sended,"+bytes)); 

			output.collect(new Text(ip_dest+":"+ip_source), new Text("recv,"+bytes)); 
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
			String keyD = key.toString();
			HashMap<String, HashMap<String,Long>> s = new HashMap<>();
			//int val=Integer.MIN_VALUE; 
			while (values.hasNext()){ 
				String va = values.next().toString();
				//System.out.println(va);
				String[] o = va.split(",");
				HashMap<String,Long> has = new HashMap<>();
				if (s.containsKey(keyD)){
					if (o.length == 4){
						s.get(keyD).put(o[0], s.get(keyD).get(o[0]) + Long.parseLong(o[1]));
						s.get(keyD).put(o[2], s.get(keyD).get(o[2]) + Long.parseLong(o[3]));
					}else{
						s.get(keyD).put(o[0], s.get(keyD).get(o[0]) + Long.parseLong(o[1]));
					}
				}else{
					if (o.length == 4){
						has.put(o[0],Long.parseLong(o[1]));
						has.put(o[2],Long.parseLong(o[3]));
						s.put(keyD.toString(),has);
					}else{	
						if (o[0].equals("sended")){
							has.put(o[0],Long.parseLong(o[1]));
							has.put("recv",(long)0);
							
						}else{
							has.put(o[0],Long.parseLong(o[1]));
							has.put("sended",(long)0);							
						}
						s.put(keyD.toString(),has);
					}
				}
			}
			StringBuilder string = new StringBuilder();
			for ( String val: s.keySet()){
				string.append("sended,"+s.get(val).get("sended")+",recv,"+s.get(val).get("recv"));
				}
		//	System.out.println(string);
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
			conf.setMapperClass(E_EMapperRECV.class); 
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
