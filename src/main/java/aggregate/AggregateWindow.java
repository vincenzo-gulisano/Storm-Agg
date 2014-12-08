package aggregate;

import java.util.List;

import storm.trident.tuple.TridentTuple;

public interface AggregateWindow {

	public AggregateWindow factory();

	public void setup();
	
	public void update(TridentTuple t);

	public List<Object> getAggregatedResult(long timestamp, String groupby);

}
