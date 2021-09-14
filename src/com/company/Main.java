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

        String fReplaceString = "SELECT * FROM player_cases";
        ResultSet fResultSet = fDbConnection.prepareStatement(fReplaceString).executeQuery();

        while(fResultSet.next()) {
            String insertString = "INSERT INTO player_cases (" + Const.NAME +", "
                    + Const.CASES + ", "
                    + Const.OPEN_CASES_FACTOR + ", "
                    + Const.LAST_CHANGE_OPEN + ", "
                    + Const.SERVER_NAME + ") VALUES (?, ?, ?, ?, ?)";
            PreparedStatement prSt = yDbConnection.prepareStatement(insertString);
            prSt.setString(1, fResultSet.getString(Const.NAME));
            prSt.setString(2, fResultSet.getString(Const.CASES));
            prSt.setString(3, fResultSet.getString(Const.OPEN_CASES_FACTOR));
            prSt.setString(4, fResultSet.getString(Const.LAST_CHANGE_OPEN));
            prSt.setString(5, "forestcraft");
            prSt.executeUpdate();
            replacedCases++;
        }

        String nReplaceString = "SELECT * FROM player_cases";
        ResultSet nResultSet = nDbConnection.prepareStatement(nReplaceString).executeQuery();

        while(nResultSet.next()) {
            String insertString = "INSERT INTO player_cases (" + Const.NAME +", "
                    + Const.CASES + ", "
                    + Const.OPEN_CASES_FACTOR + ", "
                    + Const.LAST_CHANGE_OPEN + ", "
                    + Const.SERVER_NAME + ") VALUES (?, ?, ?, ?, ?)";
            PreparedStatement prSt = yDbConnection.prepareStatement(insertString);
            prSt.setString(1, nResultSet.getString(Const.NAME));
            prSt.setString(2, nResultSet.getString(Const.CASES));
            prSt.setString(3, nResultSet.getString(Const.OPEN_CASES_FACTOR));
            prSt.setString(4, nResultSet.getString(Const.LAST_CHANGE_OPEN));
            prSt.setString(5, "nexting");
            prSt.executeUpdate();
            replacedCases++;
        }

        return replacedCases;
    }
}
