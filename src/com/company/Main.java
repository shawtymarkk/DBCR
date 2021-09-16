package com.company;

import java.sql.*;

public class Main extends Configs {

    static Connection yDbConnection;
    static Connection nDbConnection;
    static Connection fDbConnection;

    public static void main(String[] args) throws SQLException {
        try {
            yDbConnection = getDbConnection(yDBHost, yDBPort, yDBUser, yDBPass, yDBName);
            System.out.println("Успешно подключились к бд " + yDbConnection.getCatalog());
            nDbConnection = getDbConnection(nDBHost, nDBPort, nDBUser, nDBPass, nDBName);
            System.out.println("Успешно подключились к бд " + nDbConnection.getCatalog());
            fDbConnection = getDbConnection(fDBHost, fDBPort, fDBUser, fDBPass, fDBName);
            System.out.println("Успешно подключились к бд " + fDbConnection.getCatalog());
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        int replacedCases = replacingCases();
        System.out.println("Перенесли: " + replacedCases + " кейсов игроков");
        yDbConnection.close();
        nDbConnection.close();
        fDbConnection.close();
    }

    public static Connection getDbConnection(String dbHost, String dbPort, String dbUser, String dbPass, String dbName)
            throws SQLException, ClassNotFoundException {

        String stringConnection = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName;

        Class.forName("com.mysql.cj.jdbc.Driver");

        Connection dbConnection = DriverManager.getConnection(stringConnection, dbUser, dbPass);

        return dbConnection;
    }

    public static int replacingCases() throws SQLException {
        int replacedCases = 0;
        int i = 0;

        String yString = "SELECT * FROM player_cases WHERE " + Const.SERVER_NAME + " = ''";
        ResultSet yResult = yDbConnection.prepareStatement(yString).executeQuery();
        while (yResult.next()) {
            String updateString = "UPDATE player_cases SET " + Const.NAME + " = ?, "
                    + Const.CASES + " = ?, "
                    + Const.OPEN_CASES_FACTOR + " = ?, "
                    + Const.LAST_CHANGE_OPEN + " = ?, "
                    + Const.SERVER_NAME + " = ? WHERE " + Const.NAME + " = ?";
            PreparedStatement prSt = yDbConnection.prepareStatement(updateString);
            prSt.setString(1, yResult.getString(Const.NAME));
            prSt.setString(2, yResult.getString(Const.CASES));
            prSt.setString(3, yResult.getString(Const.OPEN_CASES_FACTOR));
            prSt.setString(4, yResult.getString(Const.LAST_CHANGE_OPEN));
            prSt.setString(5, "yottacraft");
            prSt.setString(6, yResult.getString(Const.NAME));
            prSt.executeUpdate();
            i++;
        }
        System.out.println(i);

        String fReplaceString = "SELECT * FROM player_cases";
        ResultSet fResultSet = fDbConnection.prepareStatement(fReplaceString).executeQuery();

        while(fResultSet.next()) {
            String rowCheckingString = "SELECT * FROM player_cases WHERE " + Const.NAME + " = ? AND " + Const.SERVER_NAME + " = ?";
            PreparedStatement prst = yDbConnection.prepareStatement(rowCheckingString);
            prst.setString(1, fResultSet.getString(Const.NAME));
            prst.setString(2, "forestcraft");
            if(!prst.executeQuery().next()) {
                insertCases(fResultSet, "forestcraft");
                replacedCases++;
            }
        }

        String nReplaceString = "SELECT * FROM player_cases";
        ResultSet nResultSet = nDbConnection.prepareStatement(nReplaceString).executeQuery();

        while(nResultSet.next()) {
            String rowCheckingString = "SELECT * FROM player_cases WHERE " + Const.NAME + " = ? AND " + Const.SERVER_NAME + " = ?";
            PreparedStatement prst = yDbConnection.prepareStatement(rowCheckingString);
            prst.setString(1, nResultSet.getString(Const.NAME));
            prst.setString(2, "nexting");
            if(!prst.executeQuery().next()) {
                insertCases(nResultSet, "nexting");
                replacedCases++;
            }
        }

        return replacedCases;
    }

    public static void insertCases(ResultSet resultSet, String serverName) throws SQLException {
        String insertString = "INSERT INTO player_cases (" + Const.NAME +", "
                + Const.CASES + ", "
                + Const.OPEN_CASES_FACTOR + ", "
                + Const.LAST_CHANGE_OPEN + ", "
                + Const.SERVER_NAME + ") VALUES (?, ?, ?, ?, ?)";
        PreparedStatement prSt = yDbConnection.prepareStatement(insertString);
        prSt.setString(1, resultSet.getString(Const.NAME));
        prSt.setString(2, resultSet.getString(Const.CASES));
        prSt.setString(3, resultSet.getString(Const.OPEN_CASES_FACTOR));
        prSt.setString(4, resultSet.getString(Const.LAST_CHANGE_OPEN));
        prSt.setString(5, serverName);
        prSt.executeUpdate();
    }
}
