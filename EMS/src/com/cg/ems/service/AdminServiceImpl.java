package com.cg.ems.service;

import java.util.ArrayList;
import java.util.regex.Pattern;

import com.cg.ems.dao.AdminDao;
import com.cg.ems.dao.AdminDaoImpl;
import com.cg.ems.dto.Department;
import com.cg.ems.dto.Employee;
import com.cg.ems.dto.GradeMaster;
import com.cg.ems.exception.EmployeeException;

public class AdminServiceImpl implements AdminService
{
	AdminDao adminDao;
	
	public AdminServiceImpl() 
	{
		adminDao =new AdminDaoImpl();
	}

	@Override
	public int addEmployee(Employee emp) throws EmployeeException{
	
		return adminDao.addEmployee(emp);
	}

	

	@Override
	public String getDepartment(int id) throws EmployeeException {

		return adminDao.getDepartment(id);
	}

	@Override
	public int[] salaryBracket(String id) throws EmployeeException {
		return adminDao.salaryBracket(id);
	}

	@Override
	public boolean validateSalary(int minSal, int maxSal, int selSal) throws EmployeeException 
	{
		if(selSal>=minSal && selSal<=maxSal)
		{
			return true;
		}
		else
		{
			return false;
			//throw new EmployeeException("Salary should be within "+minSal+" - "+maxSal+" bracket");
		}
		
	}
	@Override
	public boolean validateEmpId(String empId) throws EmployeeException
	{
		String empIdPattern="[0-9]{6}";
		if(Pattern.matches( empIdPattern,empId))
		{
			return true;
		}
		else
		{
			//throw new EmployeeException("Invalid Employee ID, should have only 6 digits:\n\n\n");
			return false;
		}
	}
	
	@Override
	public boolean isValidContact(long contact) throws EmployeeException
	{
		String matchPattern="[6-9]{1}[0-9]{9}";
		String contactPattern=Long.toString(contact);
		if(Pattern.matches(matchPattern, contactPattern))
		{
			return true;
		}
		else
		{
			return false;
			//throw new EmployeeException("Invalid contact number, should be 10 digits:\n\n\n");
		}
	}

	@Override
	public boolean validateName(String empName,int flag) throws EmployeeException
	{
		
		String empNamePattern="[A-Z][a-z]+";
		if(Pattern.matches(empNamePattern,empName))
		{
			return true;
		}
		else
		{
			return false;
			/*
			if(flag==1)
			{
				throw new EmployeeException("Invalid First Name, Should start with Capital:\n\n\n");
				
			}
			else
			{
				throw new EmployeeException("Invalid Last Name, Should start with Capital:\n\n\n");
				
			}
			*/
		}
	}

	@Override
	public ArrayList<Department> displayDepartment() throws EmployeeException 
	{
		return adminDao.displayDepartment();
	}

	@Override
	public ArrayList<GradeMaster> getGradeCodes() throws EmployeeException 
	{
		
		return adminDao.getGradeCodes();
	}

	@Override
	public boolean validateDeptID(ArrayList<Integer> validateDepId, int depId) throws EmployeeException
	{
		if(validateDepId.contains(depId))
		{
			return true;
		}
		else
		{
			return false;
			//throw new EmployeeException("Selected Department ID: "+depId+" is Invalid:\n\n\n");
		}
		
		
	}

	@Override
	public boolean isValidGradeCode(ArrayList<String> validateGradeCode, String gradeCode) throws EmployeeException 
	{
		if(validateGradeCode.contains(gradeCode))
		{
			return true;
		}
		else
		{
			return false;
			//throw new EmployeeException("Selected GradeCode: "+gradeCode+" is Invalid:\n\n\n");
		}
		
	}

	@Override
	public boolean isValidSalary(int[] salBracket, int eSal) throws EmployeeException 
	{
		if(salBracket[1]>=eSal && salBracket[0]<=eSal)
		{
			return true;
		}
		else
		{
			return false;
			//throw new EmployeeException("Entered Salary is not in Salary Brackets: ["+salBracket[0]+"-"+salBracket[1]+"]:\n\n\n");
		}
	}

	@Override
	public boolean isValidGender(int gen) throws EmployeeException 
	{
		if(gen<=3 && gen>=1)
		{
			return true;
		}
		else
		{
			return false;
			//throw new EmployeeException("Entered Gender in not valid:\n\n\n");
		}
		
	}

	@Override
	public boolean isValidMaritialStatus(int status) throws EmployeeException 
	{
		if(status<=5 && status>=1)
		{
			return true;
		}
		else
		{
			return false;
			//throw new EmployeeException("Entered Maritial Status in not valid:\n\n\n");
		}
	}

	@Override
	public ArrayList<Employee> displayAllEmployee() throws EmployeeException 
	{
		return adminDao.displayAllEmployee();
	}

	@Override
	public int updateEmployee(Employee e) throws EmployeeException {
		System.out.println("in service");
		return adminDao.updateEmployee(e);
	}

	@Override
	public Employee getEmployeeById(String id) throws EmployeeException {
		
		return adminDao.getEmployeeById(id);
	}

	

}
