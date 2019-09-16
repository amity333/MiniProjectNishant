package com.cg.ems.dao;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import com.cg.ems.dto.Department;
import com.cg.ems.dto.Employee;
import com.cg.ems.dto.GradeMaster;
import com.cg.ems.exception.EmployeeException;
import com.cg.ems.util.DBUtil;
import com.cg.ems.util.MyStringDateUtil;

public class AdminDaoImpl implements AdminDao {
	Connection con = null;
	Statement st = null;
	PreparedStatement pst = null;
	ResultSet rs = null;

	public void connect() {
		try {
			con = DBUtil.getCon();
		} catch (ClassNotFoundException | SQLException | IOException e) {

			e.printStackTrace();
		}
	}

	@Override
	public int addEmployee(Employee emp) throws EmployeeException {
		connect();
		int dataInserted = 0;
		try {
			pst = con.prepareStatement(QueryMapper.EMP_INSERT_QRY);
			pst.setString(1, emp.getEmpID());
			pst.setString(2, emp.getEmpFirstName());
			pst.setString(3, emp.getEmpLastName());
			pst.setDate(4, MyStringDateUtil.fromLocalToSqlDate(emp.getEmpDateofBirth()));
			pst.setDate(5, MyStringDateUtil.fromLocalToSqlDate(emp.getEmpDateofJoining()));
			// pst.setString(6, emp.getEmpDepartment());
			pst.setInt(6, emp.getEmpDepartmentId());
			pst.setString(7, emp.getEmpGrade());
			pst.setString(8, emp.getEmpDesignation());
			pst.setInt(9, emp.getEmpBasicSal());
			pst.setString(10, emp.getEmpGender());
			pst.setString(11, emp.getEmpMaritalStatus());
			pst.setString(12, emp.getEmpHomeAddress());
			pst.setLong(13, emp.getEmpContactNum());
			pst.setString(14, emp.getMgrId());
			dataInserted = pst.executeUpdate();
		} catch (SQLException e) {
			throw new EmployeeException(e.getMessage());
		}
		return dataInserted;
	}

	@Override
	public int[] salaryBracket(String id) throws EmployeeException {
		connect();
		int[] sal = new int[2];
		try {
			pst = con.prepareStatement(QueryMapper.GET_EMP_SALARY);
			pst.setString(1, id);
			rs = pst.executeQuery();
			while (rs.next()) {
				sal[0] = rs.getInt(1);
				sal[1] = rs.getInt(2);
			}
		} catch (SQLException e) {
			throw new EmployeeException(e.getMessage());
		}
		return sal;
	}

	@Override
	public String getDepartment(int id) throws EmployeeException {
		connect();
		String empDepartment = null;
		try {
			pst = con.prepareStatement(QueryMapper.GET_EMP_DEPARTMENT);
			pst.setInt(1, id);
			rs = pst.executeQuery();
			while (rs.next()) {
				empDepartment = rs.getString(1);
			}
		} catch (SQLException e) {
			throw new EmployeeException(e.getMessage());
		}
		return empDepartment;
	}

	@Override
	public ArrayList<Department> displayDepartment() throws EmployeeException {
		connect();
		ArrayList<Department> DepartmentList = new ArrayList<Department>();
		try {
			pst = con.prepareStatement(QueryMapper.GET_DEPARTMENT);
			rs = pst.executeQuery();
			Department dept;
			while (rs.next()) {
				dept = new Department(rs.getString(2), rs.getInt(1));
				DepartmentList.add(dept);
			}
		} catch (SQLException e) {
			throw new EmployeeException(e.getMessage());
		}

		return DepartmentList;
	}

	@Override
	public ArrayList<GradeMaster> getGradeCodes() throws EmployeeException {
		connect();
		ArrayList<GradeMaster> grade = new ArrayList<GradeMaster>();
		try {
			pst = con.prepareStatement(QueryMapper.GET_GRADECODE);
			rs = pst.executeQuery();
			GradeMaster details;
			while (rs.next()) {
				details = new GradeMaster(rs.getString(1), rs.getInt(3), rs.getInt(4));
				grade.add(details);
			}
		} catch (SQLException e) {
			throw new EmployeeException(e.getMessage());

		}
		return grade;
	}

	@Override
	public ArrayList<Employee> displayAllEmployee() throws EmployeeException {
		ArrayList<Employee> list = new ArrayList<Employee>();
		connect();
		try {
			st = con.createStatement();
			rs = st.executeQuery(QueryMapper.GET_ALL_EMPLOYEE);
			while (rs.next()) {
				Employee emp = new Employee(rs.getString(1), rs.getString(2), rs.getString(3),
						MyStringDateUtil.fromSqlToLocalDate(rs.getDate(4)),
						MyStringDateUtil.fromSqlToLocalDate(rs.getDate(5)), rs.getInt(6), rs.getString(7),
						rs.getString(8), rs.getInt(9), rs.getString(10), rs.getString(11), rs.getString(12),
						rs.getLong(13), rs.getString(14));
				list.add(emp);
				// System.out.println(emp);
			}
		} catch (SQLException e) {
			throw new EmployeeException(e.getMessage());

		}
		return list;
	}

	@Override
	public int updateEmployee(Employee e) throws EmployeeException {
		Employee ref = null;
		int row;
		connect();

		System.out.println("updateDao");
		try {
			String qry = "UPDATE employee SET EMP_FIRST_NAME=?, EMP_LAST_NAME=?,EMP_DEPT_ID=?,EMP_GRADE=?,"
					+ "EMP_DESIGNATION=?,EMP_BASIC=? "
					+ ",EMP_GENDER=?, EMP_MARITAL_STATUS=?,EMP_HOME_ADDRESS=?, EMP_CONTACT_NUM=? " + "where EMP_ID =?";

			pst = con.prepareStatement(qry);

			pst.setString(1, e.getEmpFirstName());
			pst.setString(2, e.getEmpLastName());
			pst.setInt(3, e.getEmpDepartmentId());
			pst.setString(4, e.getEmpGrade());
			pst.setString(5, e.getEmpDesignation());
			pst.setInt(6, e.getEmpBasicSal());
			pst.setString(7, e.getEmpGender());
			;
			pst.setString(8, e.getEmpMaritalStatus());
			pst.setString(9, e.getEmpHomeAddress());

			pst.setLong(10, e.getEmpContactNum());
			pst.setString(11, e.getEmpID());

			row = pst.executeUpdate();

			if (row > 0) {
				ref = e;
			}

		} catch (Exception ee) {
			throw new EmployeeException(ee.getMessage());
		}
		return row;

	}

	@Override
	public Employee getEmployeeById(String id) throws EmployeeException {
		connect();
		Employee ref = null;
		String qry = "SELECT * FROM employee WHERE  EMP_ID=?";
		try {
			PreparedStatement pstmt = con.prepareStatement(qry);
			pstmt.setString(1, id);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				String empId = rs.getString(1);

				String empFirstName = rs.getString(2);
				String empLastName = rs.getString(3);

				Date empDOB = rs.getDate(4);
				LocalDate empDOBLocal = MyStringDateUtil.fromSqlToLocalDate(empDOB);
				Date empDOJ = rs.getDate(5);
				LocalDate empDOJLocal = MyStringDateUtil.fromSqlToLocalDate(empDOJ);
				int empDeptId = rs.getInt(6);
				String empGrade = rs.getString(7);
				String empDesignation = rs.getString(8);
				int empBasic = rs.getInt(9);
				String empGender = rs.getString(10);
				String empMartialStatus = rs.getString(11);
				String empHomeAddress = rs.getString(12);
				long empContactNumber = rs.getLong(13);
				String mgrId = rs.getString(14);
				ref = new Employee(empId, empFirstName, empLastName, empDOBLocal, empDOJLocal, empDeptId, empGrade,
						empDesignation, empBasic, empGender, empMartialStatus, empHomeAddress, empContactNumber, mgrId);
				System.out.println(ref);
			}
		} catch (Exception e) {
			e.printStackTrace();

		}
		return ref;
	}
}
