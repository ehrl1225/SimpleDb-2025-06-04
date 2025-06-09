package com.back.simpleDb;

import lombok.Setter;

import java.sql.*;

public class SimpleDb {
    private String url;
    private String user;
    private String password;
    private String dbName;
    private ThreadLocal<Connection> conn;
    private boolean closeable;


    @Setter
    boolean devMode = false;

    public SimpleDb(String url, String user, String password, String dbName)  {
        try{
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = ThreadLocal.withInitial(()-> {
                try {
                    return DriverManager.getConnection(String.format("jdbc:mysql://%s:3306/%s", url, dbName), user, password);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
            this.url = url;
            this.user = user;
            this.password = password;
            this.dbName = dbName;
            closeable = true;

        }catch(SQLException|ClassNotFoundException e){

        }
    }

    public void run(String sql){
        try{
            Statement stmt = conn.get().createStatement();
            stmt.execute(sql);
            stmt.close();
        }catch (SQLException e){

        }

    }

    public void run(String sql, String title, String body, boolean isBlind){
        try{
            PreparedStatement stmt = conn.get().prepareStatement(sql);
            stmt.setString(1, title);
            stmt.setString(2, body);
            stmt.setBoolean(3, isBlind);
            stmt.execute();
        }catch (SQLException e){

        }
    }

    public Sql genSql(){
        try{
            Connection conn = DriverManager.getConnection(String.format("jdbc:mysql://%s:3306/%s", url, dbName), user, password);
            conn.setAutoCommit(false);
            Sql sql = new Sql(conn, closeable);
            this.conn.set(conn);
            return sql;

        }catch (SQLException e){

        }
        return null;
    }


    public void startTransaction() {
        run("start transaction");
        closeable = false;
    }

    public void rollback(){
        run("rollback");
        try{
            conn.close();
        }catch (SQLException e){
        }
    }

    public void commit(){
        run("commit");
        try{
            conn.get().close();
        }catch (SQLException e){
        }
    }

    public void close(){
        try{
            conn.get().close();

        }catch(SQLException e){

        }
    }
}
