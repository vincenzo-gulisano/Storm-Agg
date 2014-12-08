package csvio;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;

import storm.trident.tuple.TridentTuple;

public abstract class CSVFileWriter implements Serializable {

	private static final long serialVersionUID = -1692435045526854126L;
	private boolean everythingRead;
	private PrintWriter pw;

	public CSVFileWriter() {
		this.everythingRead = false;
	}

	public void setup(String fileName) {
		try {
			this.pw = new PrintWriter(new FileWriter(fileName));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public boolean everythingRead() {
		return this.everythingRead;
	}

	public void write(TridentTuple t) {
		pw.println(convertTupleToLine(t));
		pw.flush();
	}

	public void close() {
		pw.flush();
		pw.close();
	}

	protected abstract String convertTupleToLine(TridentTuple t);

}
