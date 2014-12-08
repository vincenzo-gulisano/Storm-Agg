package csvio;

import java.util.Map;

import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class CSVFunction implements Function {

	private static final long serialVersionUID = -3583695414697840600L;
	private CSVFileWriter fw;
	private String fileName;

	public CSVFunction(CSVFileWriter fw,String fileName) {
		this.fw = fw;
		this.fileName = fileName;
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map conf, TridentOperationContext context) {
		fw.setup(fileName);
	}

	public void cleanup() {
		fw.close();
	}

	public void execute(TridentTuple tuple, TridentCollector collector) {
		fw.write(tuple);
	}

}
