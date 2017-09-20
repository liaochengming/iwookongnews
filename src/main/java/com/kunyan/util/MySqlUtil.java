package com.kunyan.util;


import java.sql.*;

/**
 * Created by Administrator on 2017/9/11.
 * mysql的相关工具方法
 */
public class MySqlUtil {


    public static Connection getMysqlConn(String url,String userName,String passWord){

        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            return DriverManager.getConnection(url, userName, passWord);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

    }

    public static ResultSet getMysqlData(Connection conn,String selectStr) {

        PreparedStatement ppst = null;
        try {
            ppst = conn.prepareStatement(selectStr);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            assert ppst != null;
            return ppst.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }


}
