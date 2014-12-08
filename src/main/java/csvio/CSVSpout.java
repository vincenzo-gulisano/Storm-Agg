package csvio;

import java.util.Map;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class CSVSpout implements IBatchSpout {

	private static final long serialVersionUID = 7473829013824869049L;
	private Fields outputFields;
	private CSVFileReader fr;
	private int batchSize;
	private int sleepTime;
	private String fileName;

	public CSVSpout(Fields outputFields, int batchSize, int sleepTime,
			CSVFileReader fr, String fileName) {
		this.outputFields = outputFields;
		this.batchSize = batchSize;
		this.sleepTime = sleepTime;
		this.fr = fr;
		this.fileName = fileName;
	}

	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context) {
		fr.setup(fileName);
	}

	public void emitBatch(long batchId, TridentCollector collector) {
		for (int i = 0; i < batchSize; i++) {
			if (fr.hasNext()) {
				collector.emit(fr.getNextTuple());
			}
		}
		Utils.sleep(sleepTime);
	}

	public void ack(long batchId) {
	}

	public void close() {
		fr.close();
	}

	@SuppressWarnings("rawtypes")
	public Map getComponentConfiguration() {
		return null;
	}

	public Fields getOutputFields() {
		return this.outputFields;
	}

}
