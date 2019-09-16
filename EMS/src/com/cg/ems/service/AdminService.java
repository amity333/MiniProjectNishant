package com.cg.ems.service;

import java.util.ArrayList;

import com.cg.ems.dto.Department;
import com.cg.ems.dto.Employee;
import com.cg.ems.dto.GradeMaster;
import com.cg.ems.exception.EmployeeException;

public interface AdminService 
{
	public int addEmployee(Employee emp)throws EmployeeException;
	public Employee getEmployeeById(String id)throws EmployeeException;
	public int[] salaryBracket(String id) throws EmployeeException;
	public String getDepartment(int id) throws EmployeeException;
	public boolean validateSalary(int minSal,int maxSal,int selSal) throws EmployeeException;
	public boolean validateEmpId(String empId) throws EmployeeException;
	public boolean validateName(String empName,int flag) throws EmployeeException;
	public ArrayList<Department> displayDepartment()throws EmployeeException;
	public ArrayList <GradeMaster> getGradeCodes()throws EmployeeException;
	public boolean validateDeptID(ArrayList <Integer>validateDepId,int depId)throws EmployeeException;
	public boolean isValidGradeCode(ArrayList <String>validateGradeCode,String gradeCode)throws EmployeeException;
	public boolean isValidSalary(int[] salBracket,int eSal)throws EmployeeException;
	public boolean isValidGender(int gen)throws EmployeeException;
	public boolean isValidMaritialStatus(int status)throws EmployeeException;
	public boolean isValidContact(long contact) throws EmployeeException;
	public ArrayList <Employee> displayAllEmployee() throws EmployeeException;
	
	public int updateEmployee(Employee e) throws EmployeeException;
	
	
    
}
