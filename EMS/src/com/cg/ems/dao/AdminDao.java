package com.cg.ems.dao;

import java.util.ArrayList;

import com.cg.ems.dto.Department;
import com.cg.ems.dto.Employee;
import com.cg.ems.dto.GradeMaster;
import com.cg.ems.exception.EmployeeException;

public interface AdminDao 
{
	public int addEmployee(Employee emp)throws EmployeeException;
	public Employee getEmployeeById(String id)throws EmployeeException;
	public int[] salaryBracket(String id) throws EmployeeException;
	public String getDepartment(int id) throws EmployeeException;
	public ArrayList<Department> displayDepartment()throws EmployeeException;
	public ArrayList <GradeMaster> getGradeCodes()throws EmployeeException;
	public ArrayList <Employee> displayAllEmployee() throws EmployeeException;
	
	
	public int updateEmployee(Employee e) throws EmployeeException;
     
}
