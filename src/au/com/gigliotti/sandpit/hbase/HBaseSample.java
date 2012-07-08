package au.com.gigliotti.sandpit.hbase;

import java.util.NavigableMap;
import java.util.NavigableSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Sample HBase application which retrieves all rows from a table and then
 * prints them to the console. The table definition is as follows:
 * 
 * table: words column family: definitions
 * 
 * @author Gerard Gigliotti (gerard.gigliotti.com.au)
 * 
 * 
 */
public class HBaseSample {

	// The name of the table being queried
	public static final byte[] WORDS_TABLE = Bytes.toBytes("words");
	// The column family
	public static final byte[] DEFINITIONS_FAMILY = Bytes
			.toBytes("definitions");

	/**
	 * Construct creates a new instance of HBaseSample, and invokes the start
	 * method.
	 */
	public static void main(String[] args) throws Exception {
		(new HBaseSample()).start();
	}

	/**
	 * Method prints out all rows within the words table.
	 * */
	private void start() throws Exception {

		// Load's the hbase-site.xml config
		Configuration config = HBaseConfiguration.create();
		HTableFactory factory = new HTableFactory();
		HBaseAdmin.checkHBaseAvailable(config);

		// Link to table
		HTableInterface table = factory.createHTableInterface(config,
				WORDS_TABLE);

		// Used to retrieve rows from the table
		Scan scan = new Scan();

		// Scan through each row in the table
		ResultScanner rs = table.getScanner(scan);
		try {
			// Loop through each retrieved row
			for (Result r = rs.next(); r != null; r = rs.next()) {
				System.out.println("Key: " + new String(r.getRow()));
				/*
				 * Cycles through each qualifier within the "definitions" family.
				 * The column family, with the qualifier, make up the column
				 * name; it is usually represented in the syntax
				 * family:qualifier. In our example, each qualifier is a number.
				 */
				NavigableMap<byte[], byte[]> familyMap = r
						.getFamilyMap(DEFINITIONS_FAMILY);
				// This is a list of the qualifier keys
				NavigableSet<byte[]> keySet = familyMap.navigableKeySet();

				// Print out each value within each qualifier
				for (byte[] key : keySet) {
					System.out.println("\t Definition: " + (new String(key))
							+ ", Value:"
							+ new String(r.getValue(DEFINITIONS_FAMILY, key)));
				}
			}
		} catch (Exception e) {
			throw e;
		} finally {
			rs.close();
		}

	}
}