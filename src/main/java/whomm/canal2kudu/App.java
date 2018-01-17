package whomm.canal2kudu;




import org.apache.kudu.*;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

import java.util.ArrayList;
import java.util.List;





/**
 * Hello world! 
 * java -classpath ./canel2kudu-0.0.1-SNAPSHOT.jar whomm.canal2kudu.App
 * java  -Djava.ext.dirs=./ -DkuduMaster=127.0.0.1:7051  whomm.canal2kudu.App
 * java  -Djava.ext.dirs=./ -DkuduMaster=10.0.2.61:7051  whomm.canal2kudu.App
 * java  -Djava.ext.dirs=./ -DkuduMaster=10.0.3.188:7051  whomm.canal2kudu.App
 */



public class App {

	private static final String KUDU_MASTER = System.getProperty("kuduMaster",
			"quickstart.cloudera");
	


	public static void main(String[] args) {
		
    	    System.out.println("-----------------------------------------------");
    	    System.out.println("Will try to connect to Kudu master at " + KUDU_MASTER);
    	    System.out.println("Run with -DkuduMaster=myHost:port to override.");
    	    System.out.println("-----------------------------------------------");
    	    String tableName = "testuser";
    	    KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();

    	    try {
    	      List<ColumnSchema> columns = new ArrayList<ColumnSchema>(2);
    	      columns.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32)
    	          .key(true)
    	          .build());
    	      columns.add(new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING)
    	          .build());
    	      List<String> rangeKeys = new ArrayList<String>();
    	      rangeKeys.add("id");

    	      Schema schema = new Schema(columns);
    	      
    	      client.createTable(tableName, schema,
    	                         new CreateTableOptions().setRangePartitionColumns(rangeKeys));

    	      System.out.println("create table");
    	      
    	      KuduTable table = client.openTable(tableName);
    	      KuduSession session = client.newSession();
    	      for (int i = 0; i < 3; i++) {
    	        Insert insert = table.newInsert();
    	        PartialRow row = insert.getRow();
    	        row.addInt(0, i);
    	        row.addString(1, "value " + i);
    	        session.apply(insert);
    	      }
    	      System.out.println("insert table");
	
    	      List<String> projectColumns = new ArrayList<String>(1);
    	      projectColumns.add("name");
    	      
    	      
    	      
    	      KuduScanner scanner = client.newScannerBuilder(table)
    	          .setProjectedColumnNames(projectColumns)
    	          .build();
    	      
    	      System.out.println("build scanner");
    	      while (scanner.hasMoreRows()) {
    	        RowResultIterator results = scanner.nextRows();
    	        while (results.hasNext()) {
    	          RowResult result = results.next();
    	          System.out.println(result.getString(0));
    	        }
    	      }
    	    } catch (Exception e) {
    	      e.printStackTrace();
    	      System.out.println("error ......");
    	    } finally {
    	      try {
    	        //client.deleteTable(tableName);
  	        	System.out.println("deletetable");

    	      } catch (Exception e) {
  	        	System.out.println("deletetable  error ......");
    	        e.printStackTrace();
    	      } finally {
    	        try {
    	          client.shutdown();
    	        } catch (Exception e) {
    	        	System.out.println("shutdown error ......");
    	          e.printStackTrace();
    	        }
    	      }
    	    }
    	    
    	  }
    	  
		
}

