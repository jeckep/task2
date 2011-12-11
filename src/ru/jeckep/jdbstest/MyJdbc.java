package ru.jeckep.jdbstest;

import java.sql.*;

public class MyJdbc {

	public static void main(String argv[]) {
		Connection conn = null;
		Statement stmt = null;
		Statement stmt2 = null;
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		ResultSet prs = null;

		try {

			conn = DriverManager.getConnection("jdbc:derby://localhost:1527/Bank;create=true");
			stmt = conn.createStatement();
			//Create tables
			String strAccounts = "CREATE TABLE BankAccounts(AccountNo int NOT NULL,Name varchar(50),Balance int NOT NULL)";
			String strClients = "CREATE TABLE BankClients(Name varchar(50),NumOfAcc int NOT NULL)";
			String initAccounts = "INSERT INTO BankAccounts values (12345,'Vasiliy Smirnov',1000000), (21345,'Dmitriy Sidrov',100),(21346,'Dmitriy Sidrov',40), (56345,'Petr Kozlov', 1500)";
			String initClients  = "INSERT INTO BankClients values ('Vasiliy Smirnov',1), ('Dmitriy Sidrov',2), ('Petr Kozlov', 1)";
			try {
			stmt.executeUpdate(strClients);
			stmt.executeUpdate(strAccounts);
			stmt.executeUpdate(initClients);
			stmt.executeUpdate(initAccounts);
			} catch(Exception e)
			{
				e.printStackTrace();
			}
			
			
//part 1---------------------------------------------------
			System.out.println("Part 1:");
			String sqlQuery = "SELECT * FROM BankAccounts WHERE Balance > 100";
			rs = stmt.executeQuery(sqlQuery);
			while (rs.next()) {
				int accountNo = rs.getInt("AccountNo");
				String name = rs.getString("Name");
				int balance = rs.getInt("Balance");
				System.out.println("" + accountNo + ", " + name + ", " + balance);
			}
//part 2---------------------------------------------------
			System.out.println("Part 2:");
			sqlQuery = "SELECT * FROM BankAccounts WHERE Balance > ?";
			pstmt = conn.prepareStatement(sqlQuery);
			pstmt.setInt(1, 50);
			prs = pstmt.executeQuery();
			while (prs.next()) {
				System.out.println("" + prs.getInt("AccountNo") + ", " + prs.getString("Name") + ", " + prs.getInt("Balance"));
			}
			
//part 3---------------------------------------------------
			System.out.println("Part 3:");
			try {
				conn.setAutoCommit(false);
				stmt2 = conn.createStatement();
				stmt2.addBatch("INSERT INTO BankClients values ('Ivan Ivanov',2)");
				stmt2.addBatch("INSERT INTO BankAccounts values (76456,'Ivan Ivanov',1000)");
				stmt2.addBatch("INSERT INTO BankAccounts values (76488,'Ivan Ivanov',2000)");
				stmt2.executeBatch();
				conn.commit();
				System.out.println("Client was succesfully added and accounts were created.");
				
			} catch (SQLException e) {
				conn.rollback();
				e.printStackTrace();
			}
			
			
//---------------------------------------------------------
		} catch (SQLException se) {
			System.out.println("SQLError: " + se.getMessage() + " code: "
					+ se.getErrorCode());

		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				rs.close();
				prs.close();
				stmt.close();
				stmt2.close();
				pstmt.close();
				conn.close();
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
