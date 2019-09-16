package com.cg.ems.dao;

public interface QueryMapper {
	public static final String EMP_INSERT_QRY = "INSERT INTO Employee VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

	public static final String GET_EMP_SALARY = "SELECT Min_Salary,Max_Salary FROM Grade_Master WHERE "
			+ "GRADE_CODE=?";

	public static final String GET_EMP_DEPARTMENT = "SELECT Dept_Name FROM Department WHERE " + "Dept_Id=?";
	public static final String GET_DEPARTMENT = "SELECT * FROM Department";

	public static final String GET_GRADECODE = "SELECT * FROM Grade_Master";
	public static final String GET_ALL_EMPLOYEE = "SELECT * FROM Employee";

	public static final String UPDATE_Employee = "UPDATE employee SET EMP_FIRST_NAME=?, EMP_LAST_NAME=?,EMP_DEPT_ID=?,EMP_GRADE=?,"
			+ "EMP_DESIGNATION=?,EMP_BASIC=?,EMP_GENDER=?, EMP_MARITAL_STATUS=?,EMP_HOME_ADDRESS=?,EMP_CONTACT_NUM=?"
			+ "where EMP_ID =?";

}