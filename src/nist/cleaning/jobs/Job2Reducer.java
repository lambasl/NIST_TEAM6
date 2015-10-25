package nist.cleaning.jobs;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job2Reducer extends Reducer<Text, Text, Text, Text> {
	private Text outKey = new Text();
	private Text outValue = new Text();
	// sdf for two given formats
	SimpleDateFormat format1 = null;
	SimpleDateFormat format2 = null;

	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context)
		throws IOException, InterruptedException {
		format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	}

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		SortedMap<Date, StringBuilder> dataMap = new TreeMap<Date, StringBuilder>();
		HashSet<String> keys = new HashSet<String>();
		for (Text v : values) {
			StringBuilder str = new StringBuilder(v.toString());
			String[] valueString = v.toString().split("\\|");
			//----------------
//			String lane_id = valueString[0];
			String datetime = valueString[1];
			String lineNumber = valueString[7];
			keys.add(lineNumber);
			Date date = null;
			try {
				date = format1.parse(datetime);
			} catch (ParseException e) {
				try {
					date = format2.parse(datetime);
				} catch (ParseException e1) {
					System.out.println("ERROR$$$$$$$$$$ : Both parsing failed for date " + datetime);
				}

			}
			if (date != null)
				dataMap.put(date, str);
		}
		Calendar cal = Calendar.getInstance(); // creates calendar
		Iterator<Date> keyIter = dataMap.keySet().iterator();
		while (keyIter.hasNext()) {
			Date keyDate = keyIter.next();
			StringBuilder valueText = dataMap.get(keyDate);
			cal.setTime(keyDate); // sets calendar time/date
			cal.add(Calendar.MINUTE, -1 * Utils.LOOKBACK_MINUTES); // subtract
																	// LOOKBACK_MINS
			Date startDate = cal.getTime();

			cal.setTime(keyDate); // sets calendar time/date
			cal.add(Calendar.MINUTE, Utils.LOOKBACK_MINUTES); // adds
																// LOOKBACK_MINS
			Date endDate = cal.getTime();
			Map<Date, StringBuilder> subMap = dataMap.subMap(startDate, endDate);
			double avgFlow = 0.0;
			if (subMap.size() > 0) {
				Iterator<Date> subMapIter = subMap.keySet().iterator();
				while (subMapIter.hasNext()) {
					Date nextKey = subMapIter.next();
					String nextval = subMap.get(nextKey).toString();
					String[] vals = nextval.split("\\|");
					//check if flag is 1 then only consider this flow value
					int flag = Integer.valueOf(vals[5]);
					if(flag ==1){
						avgFlow += Integer.parseInt(vals[3]);
					}
				}
				avgFlow = avgFlow/subMap.size();
			}
			cal.setTime(keyDate);
			//String keyString = String.valueOf(cal.get(cal.YEAR)) + String.valueOf(cal.get(cal.MONTH));
			valueText.append("|").append(String.valueOf(avgFlow));
			//outKey.set(keyString);
			outValue.set(valueText.toString());
			for(String k : keys){
				outKey.set(k);
				context.write(outKey, outValue);
			}
			
		}

	}

}
