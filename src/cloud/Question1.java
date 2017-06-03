package cloud;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Question1 {

	
	public static class E_EMapper extends MapReduceBase implements Mapper<LongWritable ,Text,Text,Text>	{ 

		//Map function 
		public void map(LongWritable key, Text value,OutputCollector<Text, Text> output,Reporter reporter) throws IOException { 
			String line = value.toString(); 
			String bytes = null;
			String time = null;
			StringTokenizer s = new StringTokenizer(line,","); 
			String source_IP = s.nextToken(); 
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

			
			long timestamp = (long) Double.parseDouble(time);
			java.util.Date dt = new Date(timestamp*1000);
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/YYYY");
			output.collect(new Text(source_IP), new Text(simpleDateFormat.format(dt)+","+bytes)); 
		} 
	}
	
	public static class CombineMap extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			
			HashMap<String, Long> combinados = new HashMap<>();
			
			while (values.hasNext()){
				String va = values.next().toString();
				String[] o = va.split("ss,");
				int leng = 0;
				if (o.length % 2 == 0){
					leng = o.length;
				}else{
					leng = o.length-1;
				}
				for (int a = 0; a < leng; a+=2){
					if (combinados.containsKey(o[a])){
						combinados.put(o[a], combinados.get(o[a])+Long.parseLong(o[a+1]));
					}else if(!combinados.containsKey(o[a])){
						combinados.put(o[a], Long.parseLong(o[a+1]));
					}
				}

			} 
			StringBuilder string = new StringBuilder();
			StringBuilder BydayTotal = new StringBuilder();
			StringBuilder BydayAVG = new StringBuilder();
			int vir = 1;
			long avg = 0;
			for ( java.util.Map.Entry<String, Long> val: combinados.entrySet()){
				if (vir == 1){
					BydayAVG.append(key+",");
					BydayTotal.append(key+",");

				}
				string.append(val.getKey()+ ","+val.getValue());
				BydayTotal.append(val.getKey()+ ","+val.getValue());
				output.collect(new Text("Total"), new Text(BydayTotal.toString()));
				avg += val.getValue();
				if ( ! (vir == combinados.size())){
					string.append(",");
				}
				
				if ( vir == combinados.size())
					string.append(", "+avg/combinados.size());    

				vir ++;

			}
			//System.out.println(string);
			output.collect(new Text("AVG"), new Text(BydayAVG.toString()));
			output.collect(key, new Text(string.toString()));
		}
		
	}
	
	public static class ReduceTOP extends MapReduceBase implements Reducer<Text,Text,Text,Text>{

		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			HashMap<String, String> byDay = new HashMap<>();
			//HashMap<String, Long> 
			if (key.equals(new Text("Total"))){
				while(values.hasNext()){
					String[] val = values.next().toString().split(",");
					if (byDay.containsKey(val[1])){
						byDay.put(val[1], byDay.get(val[1]) + val[2]);
					}else{
						byDay.put(val[1], val[2]);
					}
				}
				
			}
			
			if (key.equals(new Text("AVG"))){
				
			}
		}
		
	}
}
