package whomm.canal2kudu;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.SystemUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.CanalEntry.TransactionBegin;
import com.alibaba.otter.canal.protocol.CanalEntry.TransactionEnd;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

/*
 * 
 * 
 java  -Djava.ext.dirs=./ -DDestination=datares -DKuduMaster=10.0.2.61:7051 -DCanalSevZk=10.0.2.61:2181 -DBatchNum=10 -DKdTableName=testuser  -DDbTableName=xgooduser  whomm.canal2kudu.CanalClient
 */
public class CanalClient {

	// kudu的master
	private static final String KUDU_MASTER = System.getProperty("KuduMaster",
			"quickstart.cloudera");
	// canal server zk 配置
	private static final String CANALSERVERZK = System.getProperty(
			"CanalSevZk", "");
	// canal server destination
	private static final String DESTINATION = System.getProperty("Destination",
			"");

	// 表格名称
	private static final String DBTABLENAME = System.getProperty("DbTableName",
			"");
	// 表格名称
	private static final String KDTABLENAME = System.getProperty("KdTableName",
			"");

	// 单次读取数据条数
	private static final String BATCHNUM = System.getProperty("BatchNum",
			"1024");

	private static final String[] idStrList = { "id", "ID", "Id", "iD" };

	public static void main(String args[]) {

		// 基于zookeeper动态获取canal server的地址，建立链接，其中一台server发生crash，可以支持failover
		// 创建带cluster模式的客户端链接，自动完成failover切换，服务器列表自动扫描
		CanalConnector connector = CanalConnectors.newClusterConnector(
				CANALSERVERZK, DESTINATION, "", "");

		final CanalClient clientTest = new CanalClient();
		clientTest.setConnector(connector);
		clientTest.start();

		Runtime.getRuntime().addShutdownHook(new Thread() {

			public void run() {
				try {
					logger.info("## stop the canal client");
					clientTest.stop();
				} catch (Throwable e) {
					logger.warn(
							"##something goes wrong when stopping canal:\n{}",
							ExceptionUtils.getFullStackTrace(e));
				} finally {
					logger.info("## canal client is down.");
				}
			}

		});
	}

	protected final static Logger logger = LoggerFactory
			.getLogger(CanalClient.class);
	protected static final String SEP = SystemUtils.LINE_SEPARATOR;
	protected static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	protected volatile boolean running = false;
	protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {

		public void uncaughtException(Thread t, Throwable e) {
			logger.error("parse events has an error", e);
		}
	};
	protected Thread thread = null;
	protected CanalConnector connector;

	public CanalClient() {
	}

	protected void start() {
		Assert.notNull(connector, "connector is null");
		thread = new Thread(new Runnable() {

			public void run() {
				process();
			}
		});

		thread.setUncaughtExceptionHandler(handler);
		thread.start();
		running = true;
	}

	protected void stop() {
		if (!running) {
			return;
		}
		running = false;
		if (thread != null) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				// ignore
			}
		}

	}

	protected void process() {
		int batchSize = Integer.parseInt(BATCHNUM);
		while (running) {
			try {
				connector.connect();
				connector.subscribe();

				while (running) {
					Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
					long batchId = message.getId();
					try {
						int size = message.getEntries().size();
						if (batchId == -1 || size == 0) {
							try {
								logger.debug("get nothing waiting");
								Thread.sleep(100);
							} catch (InterruptedException e) {
							}
						} else {

							KuduEntry(message.getEntries());
							logger.debug("process ok");
						}

						connector.ack(batchId); // 提交确认

					} catch (IllegalStateException e4) {
						//kudu 数据类型
						e4.printStackTrace();
						connector.rollback(batchId); // 处理失败, 回滚数据
						running = false;
						
					} catch (KuduException e3) {
						//kudu 的异常
						e3.printStackTrace();
						connector.rollback(batchId); // 处理失败, 回滚数据
						running = false;
					} catch (Exception e) {
						e.printStackTrace();
						connector.rollback(batchId); // 处理失败, 回滚数据
						break;
					}

				}

			} catch (Exception e) {
				logger.error("process error!", e);
			} finally {
				connector.disconnect();
			}
		}
	}

	protected String buildPositionForDump(Entry entry) {
		long time = entry.getHeader().getExecuteTime();
		Date date = new Date(time);
		SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
		return entry.getHeader().getLogfileName() + ":"
				+ entry.getHeader().getLogfileOffset() + ":"
				+ entry.getHeader().getExecuteTime() + "("
				+ format.format(date) + ")";
	}

	private void KuduEntry(List<Entry> entrys) throws Exception {

		KuduClient kdclient = new KuduClient.KuduClientBuilder(KUDU_MASTER)
				.build();

		KuduSession kdsession = kdclient.newSession();

		for (Entry entry : entrys) {

			logger.info(buildPositionForDump(entry));
			if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN
					|| entry.getEntryType() == EntryType.TRANSACTIONEND) {
				logger.info("filter TRANSACTIONBEGIN continue");
				continue;
			}
			String tablename = entry.getHeader().getTableName();
			// 过滤对应的数据库表格
			if (!DBTABLENAME.equals("") && !tablename.equals(DBTABLENAME)) {
				logger.info("filter table :" + DBTABLENAME + " continue");
				continue;
			}

			String kdtablename = KDTABLENAME.equals("") ? tablename
					: KDTABLENAME;
			logger.debug("open kudu table :" + kdtablename);
			KuduTable kuduTable;
			try {
				kuduTable = kdclient.openTable(kdtablename);
			} catch (KuduException e1) {
				throw e1;
			}

			RowChange rowChage = null;
			try {

				rowChage = RowChange.parseFrom(entry.getStoreValue());
			} catch (Exception e) {
				throw new RuntimeException(
						"ERROR ## parser of eromanga-event has an error , data:"
								+ entry.toString(), e);
			}

			EventType eventType = rowChage.getEventType();

			if (eventType == EventType.DELETE || eventType == EventType.UPDATE
					|| eventType == EventType.INSERT) {
				for (RowData rowData : rowChage.getRowDatasList()) {

					if (eventType == EventType.DELETE) {

						// find Id column and delete by it
						for (Column column : rowData.getBeforeColumnsList()) {
							if (column.getName().equalsIgnoreCase("id")) {
								org.apache.kudu.client.Delete KdDelete = kuduTable
										.newDelete();
								PartialRow row = KdDelete.getRow();
								Schema colSchema = kuduTable.getSchema();
								int idindex = -1;

								for (String idname : idStrList) {
									try {
										idindex = colSchema
												.getColumnIndex(idname);
									} catch (IllegalArgumentException e) {
										continue;
									}
								}

								if (idindex >= 0) {
									switch (colSchema.getColumnByIndex(idindex)
											.getType().getDataType()) {

									case BOOL:
										row.addBoolean(idindex,
												Boolean.parseBoolean(column
														.getValue()));
										break;
									case FLOAT:
										row.addFloat(idindex, Float
												.parseFloat(column.getValue()));
										break;
									case DOUBLE:
										row.addDouble(idindex, Double
												.parseDouble(column.getValue()));
										break;
									case BINARY:
										row.addBinary(idindex, column
												.getValue().getBytes());
										break;
									case INT8:
									case INT16:
										row.addShort(idindex, Short
												.parseShort(column.getValue()));
									case INT32:
										row.addInt(idindex, Integer
												.parseInt(column.getValue()));
										break;
									case INT64:
										row.addLong(idindex, Long
												.parseLong(column.getValue()));
										break;
									case STRING:
										row.addString(idindex,
												column.getValue());
										break;
									default:
										throw new IllegalStateException(
												String.format(
														"unknown column type %s",
														colSchema
																.getColumnByIndex(
																		idindex)
																.getType()
																.getDataType()));

									}
									kdsession.apply(KdDelete);
									break;

								}

								throw new IllegalStateException(
										"cannot find index id");
							}
						}

					} else if (eventType == EventType.INSERT
							|| eventType == EventType.UPDATE) {
						// find Id column and delete by it
						org.apache.kudu.client.Upsert KdUpset = kuduTable
								.newUpsert();
						PartialRow row = KdUpset.getRow();
						Schema colSchema = kuduTable.getSchema();

						for (Column column : rowData.getAfterColumnsList()) {

							int colIdx = colSchema.getColumnIndex(column
									.getName());
							Type colType = colSchema.getColumnByIndex(colIdx)
									.getType();

							switch (colType.getDataType()) {
							case BOOL:
								row.addBoolean(colIdx,
										Boolean.parseBoolean(column.getValue()));
								break;
							case FLOAT:
								row.addFloat(colIdx,
										Float.parseFloat(column.getValue()));
								break;
							case DOUBLE:
								row.addDouble(colIdx,
										Double.parseDouble(column.getValue()));
								break;
							case BINARY:
								row.addBinary(colIdx, column.getValue()
										.getBytes());
								break;
							case INT8:
							case INT16:
								row.addShort(colIdx,
										Short.parseShort(column.getValue()));
							case INT32:
								row.addInt(colIdx,
										Integer.parseInt(column.getValue()));
								break;
							case INT64:
								row.addLong(colIdx,
										Long.parseLong(column.getValue()));
								break;
							case STRING:
								row.addString(colIdx, column.getValue());
								break;
							default:
								throw new IllegalStateException(String.format(
										"unknown column type %s", colType));
							}

						}
						kdsession.apply(KdUpset);
					}

					else {
						continue;
					}

				}

			}

			kdsession.flush();
			kdsession.close();
			kdclient.close();

		}

	}

	public void setConnector(CanalConnector connector) {
		this.connector = connector;
	}

}
