package NetworkEvaluation;

import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.BoltExecuteInfo;
import backtype.storm.task.TopologyContext;

import java.io.FileWriter;
import java.util.Map;

public class HookFinalBolt extends BaseTaskHook {
	
	String homePath;
	long timestamp;
	long cycle = 15000;
	StringBuilder latencyResultString;
	String componentID;
	
	@Override
    public void prepare(Map conf, TopologyContext context) {
		homePath = System.getenv("HOME");
		timestamp = System.currentTimeMillis();
		latencyResultString = new StringBuilder();
		componentID = context.getThisComponentId();
    }
	
	@Override
	public void boltExecute(BoltExecuteInfo info) {

        if (info.tuple != null) {
			long currentTimestamp = System.currentTimeMillis();
			long emissionTimestamp = info.tuple.getLong(2);

			latencyResultString.append(componentID + "," + emissionTimestamp + "," + currentTimestamp + "\n");

			if(currentTimestamp - timestamp > cycle) {

				timestamp = currentTimestamp;

				try {
					FileWriter writer = new FileWriter(homePath + "/Bolt-FinalLatencyHook.csv", true);
					writer.write(latencyResultString.toString());
					writer.close();
					latencyResultString.setLength(0);
				} catch(Exception e) {

				}
        	}
        }
        super.boltExecute(info);
    }
}
