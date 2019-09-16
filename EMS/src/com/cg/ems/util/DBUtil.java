package com.cg.ems.util;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DBUtil {
	static String unm;
	static String pwd;
	static String url;
	static String driver;
	
	public static Connection getCon() throws ClassNotFoundException, SQLException, IOException {
		Connection con = null;
		Properties dbProps = getDBInfo();
		unm = dbProps.getProperty("dbUserName");
		pwd = dbProps.getProperty("dbPassword");
		url = dbProps.getProperty("dbUrl");
		driver = dbProps.getProperty("dbDriver");
		Class.forName("oracle.jdbc.driver.OracleDriver");
		con = DriverManager.getConnection(url,unm,pwd);
		return con;
	}
	public static Properties getDBInfo() throws IOException{
		FileReader fr = new FileReader("dbInfo.properties");
		Properties myprops = new Properties();
		myprops.load(fr);
		return myprops;
	}
}
