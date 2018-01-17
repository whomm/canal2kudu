package whomm.canal2kudu;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DBHelper {
	public Connection conn = null;
	public PreparedStatement pst = null;

	public DBHelper(String sql, String url, String user, String password) {
		try {
			conn = DriverManager.getConnection(url, user, password);
			pst = conn.prepareStatement(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void close() {
		try {
			this.conn.close();
			this.pst.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
