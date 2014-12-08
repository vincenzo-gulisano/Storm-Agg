package example;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.tuple.TridentTuple;
import aggregate.AggregateWindow;
import aggregate.StormAggregate;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import csvio.CSVFileReader;
import csvio.CSVFileWriter;
import csvio.CSVFunction;
import csvio.CSVSpout;

public class StormAgg {

	@SuppressWarnings("serial")
	public static void main(String[] args) {

		TridentTopology topology = new TridentTopology();

		// Utility to create a Stream from a csv file

		Stream inputStream = topology.newStream("Injector", new CSVSpout(
				new Fields("ts", "x", "y"), 10, 1000, new CSVFileReader() {

					@Override
					protected List<Object> convertLineToTuple(String line) {
						return Arrays.asList(
								(Object) Long.valueOf(line.split(",")[0]),
								line.split(",")[1],
								Integer.valueOf(line.split(",")[2]));
					}
				}, "input.csv"));

		// First thing, define a class that implements AggregateWindow. You can
		// use this class to specify how you want your tuples to be aggregated.

		class SumAgg implements AggregateWindow, Serializable {

			private int sum;

			public AggregateWindow factory() {
				return new SumAgg();
			}

			public void setup() {
				sum = 0;
			}

			public void update(TridentTuple t) {
				sum += t.getIntegerByField("y");
			}

			public List<Object> getAggregatedResult(long timestamp,
					String groupby) {
				return Arrays.asList(timestamp, (Object) groupby, sum);
			}
		}

		// In order to create the Aggregate operator, create a new instance of
		// StormAggregate passing as a type your implementation of the
		// AggregateWindow. The parameters you have to pass are the following
		// ones:

		// Field containing the timestamp of the tuple
		// Field containing the groupby of the tuple (or "")
		// Window size (in time units, as represented by the tuples' timestamps)
		// Window advance (in time units, as represented by the tuples'
		// timestamps)
		// Instance of the class implementing the AggregateWindow

		Stream aggStream = inputStream.aggregate(new Fields("ts", "x", "y"),
				new StormAggregate<SumAgg>("ts", "x", 10, 5, new SumAgg()),
				new Fields("ts", "x", "sum"));

		// Utility to write a Stream to a csv file (notice that out file is
		// flushed at each new tuple!)

		aggStream.each(new Fields("ts", "x", "sum"), new CSVFunction(
				new CSVFileWriter() {
					@Override
					protected String convertTupleToLine(TridentTuple t) {
						return t.toString();
					}
				}, "output.csv"),
				new Fields());

		Config conf = new Config();
		conf.setDebug(false);

		LocalCluster cluster = new LocalCluster();
		System.out.println("Submitting topology");
		cluster.submitTopology("dpds", conf, topology.build());
		backtype.storm.utils.Utils.sleep(120000);
		System.out.println("Killing topology");
		cluster.killTopology("dpds");
		cluster.shutdown();
		System.out.println("Cluster down...");
		System.exit(0);
	}
}
