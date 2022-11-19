package NetworkEvaluation;

import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.hooks.info.SpoutAckInfo;
import backtype.storm.task.TopologyContext;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Map;

public class HookSpout extends BaseTaskHook {

	//private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SOEBasicHook.class);
	
	String homePath;
	long now;
	long timeStamp;
	long cycle = 2000;
	long counter = 0;
	long printCycle = 5;
	ArrayList<Long> latencyCompleteTimeList;
	StringBuilder latencyResultString;
	StringBuilder counterResultString;
	
	long Ackcounter = 0;
	
	long timeStamp2;
	long now2;
	long counter2 = 0;
	StringBuilder emitResultString;
	long Emitcounter = 0;
	
	int port;
	
	@Override
    public void prepare(Map conf, TopologyContext context) {
	 	/*
        //Map from stream id to component id to the Grouping used.
        Map<String, Map<String, Grouping>> targets = context.getThisTargets();
        for (Map.Entry<String, Map<String, Grouping>> entry : targets.entrySet()) {
            for (String componentId : entry.getValue().keySet()) {
            	
            }
        }
        */
		homePath = System.getenv("HOME");
		now = System.currentTimeMillis();
		now2 = System.currentTimeMillis();
		timeStamp = now;
		timeStamp2 = now;
		latencyCompleteTimeList = new ArrayList<Long>();
		latencyResultString = new StringBuilder();
		counterResultString = new StringBuilder();
		emitResultString = new StringBuilder();
		
		port = context.getThisWorkerPort();
    }
	
	@Override
	public void emit(EmitInfo info) {
		
		if (info.outTasks != null) {
			Emitcounter++;
        	
        	timeStamp2 = System.currentTimeMillis();
        	if(timeStamp2-now2 > cycle)
        	{
        		counter2++;
        		long timeMod = timeStamp2 - (timeStamp2 % cycle);
        		        	
        		emitResultString.append(port + ","+ timeMod + ",Emitted," + Emitcounter + "\n");
        		
        		now2 = timeStamp2;
        		Emitcounter = 0;
        		
        		if(counter2 >= printCycle)
        		{
        			try {
        				FileWriter writer = new FileWriter(homePath + "/Spout-EmitCounter.csv", true);
        				writer.write(emitResultString.toString());
        				writer.close();
        				
        			}catch(Exception e){e.printStackTrace();}
        			        			
        			emitResultString.setLength(0);
        			counter2 = 0;
        		}
        	}
        }
		
		super.emit(info);
	}
	
	@Override
    public void spoutAck(SpoutAckInfo info) {
		
        if (info.completeLatencyMs != null) {
        	latencyCompleteTimeList.add(info.completeLatencyMs);
        	Ackcounter++;
        	
        	timeStamp = System.currentTimeMillis();
        	if(timeStamp-now > cycle)
        	{
        		counter++;
        		long timeMod = timeStamp - (timeStamp % cycle);
        		double ltAverage = average(latencyCompleteTimeList);
        		        	
        		//LOG.info(timeMod + ",Average," + ltAverage);
        		latencyResultString.append(port + ","+ timeMod + ",Average," + ltAverage + "\n");
        		counterResultString.append(port + ","+ timeMod + ",AckCounter," + Ackcounter + "\n");
        		
        		now = timeStamp;
        		latencyCompleteTimeList.clear();
        		Ackcounter = 0;
        		
        		if(counter >= printCycle)
        		{
        			try {
        				FileWriter writer = new FileWriter(homePath + "/Spout-LatencyHook.csv", true);
        				writer.write(latencyResultString.toString());
        				writer.close();
        				
        				writer = new FileWriter(homePath + "/Spout-LatencyCounter.csv", true);
        				writer.write(counterResultString.toString());
        				writer.close();
        			}catch(Exception e){e.printStackTrace();}
        			        			
        			latencyResultString.setLength(0);
        			counterResultString.setLength(0);
        			counter = 0;
        		}
        	}
        }
    }
    
    public double average(ArrayList<Long> list) {
        // 'average' is undefined if there are no elements in the list.
        if (list == null || list.isEmpty())
            return 0.0;
        // Calculate the summation of the elements in the list
        long sum = 0;
        int n = list.size();
        // Iterating manually is faster than using an enhanced for loop.
        for (int i = 0; i < n; i++)
            sum += list.get(i);
        // We don't want to perform an integer division, so the cast is mandatory.
        return ((double) sum) / n;
    }
}
