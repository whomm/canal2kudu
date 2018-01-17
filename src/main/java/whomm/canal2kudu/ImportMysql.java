package whomm.canal2kudu;

import java.util.ArrayList;

/*
 java  -Djava.ext.dirs=./ -DKuduMaster=10.0.3.188:7051 -DMysqlStr=jdbc:mysql://127.0.0.1:8306/mytest -DDbUser=www -DDbPwd=12 -DTableName=user -DStartId=0 -DMaxId=5 -DBatchNum=1 -DThreadScount=5  whomm.canal2kudu.ImportMysql
 */

public class ImportMysql {

	// kudu的master
	private static final String KUDU_MASTER = System.getProperty("KuduMaster",
			"quickstart.cloudera");
	// 数据库链接字符串
	private static final String MYSQLSTR = System.getProperty("MysqlStr", "");

	// 数据库用户名密码
	private static final String DBUSER = System.getProperty("DbUser", "");
	private static final String DBPWD = System.getProperty("DbPwd", "");

	// 表格名称
	private static final String DBTABLENAME = System.getProperty("DbTableName",
			"");
	// 表格名称
	private static final String KDTABLENAME = System.getProperty("KdTableName",
			"");
	// 导入数据起始Id
	private static final String STARTID = System.getProperty("StartId", "0");
	// 导入数据最大Id
	private static final String MAXID = System.getProperty("MaxId", "0");

	// 单次读取数据条数
	private static final String BATCHNUM = System.getProperty("BatchNum", "10");
	// 并发线程数
	private static final String THREADSCOUNT = System.getProperty(
			"ThreadScount", "0");

	public static void main(String[] args) {

		try {
			long StartId = Long.parseLong(STARTID);
			long MaxId = Long.parseLong(MAXID);
			long Batchnum = Long.parseLong(BATCHNUM);
			long Threadscount = Long.parseLong(THREADSCOUNT);

			System.out.println(DBUSER);

			ArrayList<ImportWorker> list = new ArrayList<ImportWorker>();

			long i = 0;
			while (i < Threadscount) {
				ImportWorker worker = new ImportWorker(KUDU_MASTER, MYSQLSTR,
						DBUSER, DBPWD, DBTABLENAME, KDTABLENAME, StartId,
						MaxId, Batchnum, Threadscount);
				worker.start();
				list.add(worker);
				StartId += Batchnum;
				i++;
			}

			try {
				for (ImportWorker one : list) {
					one.join();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
