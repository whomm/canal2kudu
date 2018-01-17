package whomm.canal2kudu;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

class ImportWorker extends Thread {

	private long Threadscount = 1;
	private long Batchnum = 1;
	private long StartId = 0;
	private long MaxId = 0;
	private String DbTableName = "";
	private String KdTableName = "";
	private String MysqlStr = "";
	private String KdMaster = "";
	private String DbUser = "";
	private String DbPwd = "";
	


	protected Upsert upsertRecordToKudu(KuduTable kuduTable, ResultSet record)
			throws IllegalStateException, Exception {
		Upsert upsert = kuduTable.newUpsert();
		this.insert(kuduTable, upsert, record);
		return upsert;
	}

	protected Insert insertRecordToKudu(KuduTable kuduTable, ResultSet record)
			throws IllegalStateException, Exception {
		Insert insert = kuduTable.newInsert();
		this.insert(kuduTable, insert, record);
		return insert;
	}
	
	private void insert(KuduTable kuduTable, Operation operation,
			ResultSet record) {
		PartialRow row = operation.getRow();
		Schema colSchema = kuduTable.getSchema();
		List<ColumnSchema> colList = colSchema.getColumns();

		for (ColumnSchema col : colList) {
			String colName = col.getName();
			int colIdx = this.getColumnIndex(colSchema, colName);
			if (colIdx != -1) {
				Type colType = col.getType();

				try {
					switch (colType.getDataType()) {
					case BOOL:
						row.addBoolean(colIdx, record.getBoolean(colName));
						break;
					case FLOAT:
						row.addFloat(colIdx, record.getFloat(colName));
						break;
					case DOUBLE:
						row.addDouble(colIdx, record.getDouble(colName));
						break;
					case BINARY:
						row.addBinary(colIdx, record.getString(colName)
								.getBytes());
						break;
					case INT8:
					case INT16:
						short temp = (short) record.getShort(colName);
						row.addShort(colIdx, temp);
					case INT32:
						row.addInt(colIdx, record.getInt(colName));
						break;
					case INT64:
						row.addLong(colIdx, record.getLong(colName));
						break;
					case STRING:
						row.addString(colIdx, record.getString(colName));
						break;
					default:
						throw new IllegalStateException(String.format(
								"unknown column type %s", colType));
					}
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	private int getColumnIndex(Schema columns, String colName) {
		try {
			return columns.getColumnIndex(colName);
		} catch (Exception ex) {
			return -1;
		}
	}

	public ImportWorker(String KdMaster, String MysqlStr, String DbUser,
			String DbPwd, String DbTableName, String KdTableName, long StartId,
			long MaxId, long Batchnum, long Threadscount) {
		this.KdMaster = KdMaster;
		this.MysqlStr = MysqlStr;
		this.DbUser = DbUser;
		this.DbPwd = DbPwd;
		this.DbTableName = DbTableName;
		this.KdTableName = KdTableName;

		this.Threadscount = Threadscount;
		this.Batchnum = Batchnum;
		this.StartId = StartId;
		this.MaxId = MaxId;
	}

	@Override
	public void run() {
		try {

			KuduClient kdclient = new KuduClient.KuduClientBuilder(
					this.KdMaster).build();

			KuduTable kdtable = kdclient.openTable(this.KdTableName);
			KuduSession kdsession = kdclient.newSession();

			while (StartId < MaxId) {

				String sql = "select * from " + this.DbTableName
						+ " where id>=" + String.valueOf(StartId) + " and id<"
						+ String.valueOf(StartId + this.Batchnum);
				System.out.println(sql);
				DBHelper db = new DBHelper(sql, this.MysqlStr, this.DbUser,
						this.DbPwd);
				try {
					ResultSet ret = db.pst.executeQuery();
					ret.beforeFirst();
					while (ret.next()) {
						kdsession.apply(this.upsertRecordToKudu(kdtable, ret));
						sleep(10);
					}
					ret.close();
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				kdsession.flush();

				this.StartId = this.StartId + this.Batchnum * this.Threadscount;
			}
			
			kdsession.close();
			kdclient.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
