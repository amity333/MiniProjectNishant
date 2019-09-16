package com.cg.ems.ui;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Scanner;

import com.cg.ems.dto.Department;
import com.cg.ems.dto.Employee;
import com.cg.ems.dto.GradeMaster;
import com.cg.ems.exception.EmployeeException;
import com.cg.ems.service.AdminService;
import com.cg.ems.service.AdminServiceImpl;

public class AdminMainClassV2 
{


	static Scanner sc=new Scanner(System.in);
	static AdminService adminService=null;
	
	public void adminMainClass() 
	{
		adminService=new AdminServiceImpl();
		int choice1;
		while(true)
		{
			System.out.println("WELCOME ADMIN.....");
			System.out.println("__________________________________");
			
			System.out.println("1.Add Employee\n2.Modify Employee Details\n3.Display Employee Detials\n4.Exit");
			System.out.print("\nSelect Operation to be Performed:");
			choice1=sc.nextInt();
			switch(choice1)
			{
			case 1: addNewEmployee();
			break;
			case 2: modifyEmployee();
			break;
			case 3: displayAllEmployeeDetails();
			break;
			case 4: System.exit(1);
			break;
			default: System.out.println("Invalid Choice Entered");
			}
		}
	}

	private void displayAllEmployeeDetails() 
	{
		try
		{
			ArrayList <Employee> emp=adminService.displayAllEmployee();
			for(Employee e:emp)
			{
				System.out.println(e);
			}
		}
		catch(EmployeeException e)
		{
			System.out.println(e.getMessage());
		}
	
		
	}

	private void modifyEmployee() 
	{
		System.out.println("Enter employee Id to modify");
		String id=sc.next();
		
	
			try {
				Employee e =adminService.getEmployeeById(id);
             
				String eFName;
				do
				{
					System.out.println("Enter Employee First Name:");
					eFName=sc.next();
					if(!adminService.validateName(eFName, 1))
					{
						System.out.println("Invalid First Name, Should start with Capital:\n \n");
					}
				}while(!adminService.validateName(eFName, 1));
			
				String eLName;
				do
				{
					System.out.println("Enter Employee Last Name:");
					eLName=sc.next();
					if(!adminService.validateName(eLName, 2))
					{
						System.out.println("Invalid Last Name, Should start with Capital:\n \n");
					}
				}while(!adminService.validateName(eLName, 2));
				
				/*System.out.println("Enter Employee Date of Birth:(dd/MM/yyyy)");
				String eDob=sc.next();  
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
				LocalDate eDOB = LocalDate.parse(eDob, formatter);
				
				System.out.println("Enter Employee Date of Joining:(dd/MM/yyyy)");
				String eDoj=sc.next();
				LocalDate eDOJ=LocalDate.parse(eDoj, formatter);*/
				
				ArrayList <Department> depList=adminService.displayDepartment();
				ArrayList <Integer> validateDepId=new ArrayList<Integer>();
				System.out.println("\n-----------------------------------------------");
				System.out.println("Department ID\t\t Department Name");
				System.out.println("\n-----------------------------------------------");
				for(Department dep:depList)
				{
					System.out.println(dep.getDeptID()+"\t\t\t"+dep.getDeptName());
					validateDepId.add(dep.getDeptID());
				}
				System.out.println("-------------------------------------------------");
				
				int eDepId;
				do
				{
					System.out.println("Select Department ID:");
					eDepId=sc.nextInt();
					if(!adminService.validateDeptID(validateDepId,eDepId))
					{
						System.out.println("Selected Department ID: "+eDepId+" is Invalid:\n \n");
						
						System.out.println("-----------------------------------------------");
						System.out.println("Department ID\t\t Department Name");
						System.out.println("\n-----------------------------------------------");
						for(Department dep:depList)
						{
							System.out.println(dep.getDeptID()+"\t\t\t"+dep.getDeptName());
							validateDepId.add(dep.getDeptID());
						}
						System.out.println("-------------------------------------------------");
					}
				}while(!adminService.validateDeptID(validateDepId,eDepId));
				
				ArrayList <GradeMaster> Code=adminService.getGradeCodes();
				ArrayList <String> validateGradeCode=new ArrayList<String>();
				System.out.println("\n------------------");
				System.out.println("Grade Code");
				System.out.println("------------------");
				for(GradeMaster code:Code)
				{
					System.out.println(code.getGradeCode());
					validateGradeCode.add(code.getGradeCode());
				}
				System.out.println("------------------");
				System.out.println("Select Grade Code:");
				String eGradeCode;
				int eSal=0;
				do
				{
					eGradeCode=sc.next();
					eGradeCode=eGradeCode.toUpperCase();
					if(!adminService.isValidGradeCode(validateGradeCode,eGradeCode))
					{
						System.out.println("Selected GradeCode: "+eGradeCode+" is Invalid:\n \n");
						System.out.println("------------------");
						System.out.println("Grade Code");
						System.out.println("------------------");
						for(GradeMaster code:Code)
						{
							System.out.println(code.getGradeCode());
							validateGradeCode.add(code.getGradeCode());
						}
						System.out.println("------------------");
						System.out.println("Select Grade Code:");
					}
					else if(adminService.isValidGradeCode(validateGradeCode,eGradeCode))
					{
						int[] salBracket=new int[2];
						salBracket=adminService.salaryBracket(eGradeCode);
						
						do
						{
							System.out.println("Enter Employee basic Salary:");
							eSal=sc.nextInt();
							if(!adminService.isValidSalary(salBracket,eSal))
							{
								System.out.println("Entered Salary is not in Salary Brackets: ["+salBracket[0]+"-"+salBracket[1]+"]:\n \n ");
							}
						}while(!adminService.isValidSalary(salBracket,eSal));
					}
				}while(!adminService.isValidGradeCode(validateGradeCode,eGradeCode));
				
				System.out.println("Enter Employee Designation:");
				String eDesig=sc.next();
				
				
				int gen;
				String eGender=null;
				do
				{
					System.out.println("Select Gender:\n 1. Male \n 2. Female  \n 3. Other");
					gen=sc.nextInt();
					if(!adminService.isValidGender(gen))
					{
						System.out.println("Entered Gender in not valid:\n \n");
					}
					else if(adminService.isValidGender(gen))
					{
					
						switch(gen)
						{
						case 1: eGender="M";
						break;
						case 2: eGender="F";
						break;
						case 3: eGender="O";
						break;
						}
					}				
				}while(!adminService.isValidGender(gen));
				
				int status;
				String eMarStatus=null;
				do
				{
					System.out.println("Select Maritial Status:\n 1. Single\n 2. Married\n 3. Divorced\n 4. Seperated\n 5. Widowed");
					status=sc.nextInt();
					if(!adminService.isValidMaritialStatus(status))
					{
						System.out.println("Entered Maritial Status in not valid:\n \n");
					}
					else if(adminService.isValidMaritialStatus(status))
					{

						
						switch(status)
						{
						case 1: eMarStatus="S";
						break;
						case 2: eMarStatus="M";
						break;
						case 3: eMarStatus="D";
						break;
						case 4: eMarStatus="S";
						break;
						case 5: eMarStatus="W";
						break;
						}
					}
				}while(!adminService.isValidMaritialStatus(status));
				
				System.out.println("Enter Employee Address:");
				String eAddr=sc.next();
				
				long eContact;
				do
				{
					System.out.println("Enter Employee Contact Number:");
					eContact=sc.nextLong();
					if(!adminService.isValidContact(eContact))
					{
						System.out.println("Invalid contact number, should be 10 digits:\n \n");
					}
				}while(!adminService.isValidContact(eContact));
				
				
				Employee emp=new Employee(e.getEmpID(),eFName,eLName,e.getEmpDateofBirth(),e.getEmpDateofJoining(),eDepId,eGradeCode,eDesig,eSal,eGender,eMarStatus,eAddr,eContact,e.getMgrId()
						);

				
				adminService.updateEmployee(emp);
				if(adminService.updateEmployee(emp)==1)
				{
					System.out.println("Employee updated Successfully");
				}
				else
				{
					System.out.println("Update Operation failed");
				}
			}
			
			catch(EmployeeException ee)
			{
				System.out.println(ee.getMessage());
			}
		
		}
		
	

	private void addNewEmployee() 
	{
		try
		{
			String eId;
			do
			{
				System.out.println("Enter Employee ID:");
				eId=sc.next();	
				if(!adminService.validateEmpId(eId))
				{
					System.out.println("Employee ID Should be 6 digits:");
				}
				
			}while(!adminService.validateEmpId(eId));
		
			String eFName;
			do
			{
				System.out.println("Enter Employee First Name:");
				eFName=sc.next();
				if(!adminService.validateName(eFName, 1))
				{
					System.out.println("Invalid First Name, Should start with Capital:\n \n");
				}
			}while(!adminService.validateName(eFName, 1));
			
			String eLName;
			do
			{
				System.out.println("Enter Employee Last Name:");
				eLName=sc.next();
				if(!adminService.validateName(eLName, 2))
				{
					System.out.println("Invalid Last Name, Should start with Capital:\n \n");
				}
			}while(!adminService.validateName(eLName, 2));
			
			System.out.println("Enter Employee Date of Birth:(dd/MM/yyyy)");
			String eDob=sc.next();  
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
			LocalDate eDOB = LocalDate.parse(eDob, formatter);
			
			System.out.println("Enter Employee Date of Joining:(dd/MM/yyyy)");
			String eDoj=sc.next();
			LocalDate eDOJ=LocalDate.parse(eDoj, formatter);
			
			ArrayList <Department> depList=adminService.displayDepartment();
			ArrayList <Integer> validateDepId=new ArrayList<Integer>();
			System.out.println("\n-----------------------------------------------");
			System.out.println("Department ID\t\t Department Name");
			System.out.println("\n-----------------------------------------------");
			for(Department dep:depList)
			{
				System.out.println(dep.getDeptID()+"\t\t\t"+dep.getDeptName());
				validateDepId.add(dep.getDeptID());
			}
			System.out.println("-------------------------------------------------");
			
			int eDepId;
			do
			{
				System.out.println("Select Department ID:");
				eDepId=sc.nextInt();
				if(!adminService.validateDeptID(validateDepId,eDepId))
				{
					System.out.println("Selected Department ID: "+eDepId+" is Invalid:\n \n");
					
					System.out.println("-----------------------------------------------");
					System.out.println("Department ID\t\t Department Name");
					System.out.println("\n-----------------------------------------------");
					for(Department dep:depList)
					{
						System.out.println(dep.getDeptID()+"\t\t\t"+dep.getDeptName());
						validateDepId.add(dep.getDeptID());
					}
					System.out.println("-------------------------------------------------");
				}
			}while(!adminService.validateDeptID(validateDepId,eDepId));
			
			ArrayList <GradeMaster> Code=adminService.getGradeCodes();
			ArrayList <String> validateGradeCode=new ArrayList<String>();
			System.out.println("\n------------------");
			System.out.println("Grade Code");
			System.out.println("------------------");
			for(GradeMaster code:Code)
			{
				System.out.println(code.getGradeCode());
				validateGradeCode.add(code.getGradeCode());
			}
			System.out.println("------------------");
			System.out.println("Select Grade Code:");
			String eGradeCode;
			int eSal=0;
			do
			{
				eGradeCode=sc.next();
				eGradeCode=eGradeCode.toUpperCase();
				if(!adminService.isValidGradeCode(validateGradeCode,eGradeCode))
				{
					System.out.println("Selected GradeCode: "+eGradeCode+" is Invalid:\n \n");
					System.out.println("------------------");
					System.out.println("Grade Code");
					System.out.println("------------------");
					for(GradeMaster code:Code)
					{
						System.out.println(code.getGradeCode());
						validateGradeCode.add(code.getGradeCode());
					}
					System.out.println("------------------");
					System.out.println("Select Grade Code:");
				}
				else if(adminService.isValidGradeCode(validateGradeCode,eGradeCode))
				{
					int[] salBracket=new int[2];
					salBracket=adminService.salaryBracket(eGradeCode);
					
					do
					{
						System.out.println("Enter Employee basic Salary:");
						eSal=sc.nextInt();
						if(!adminService.isValidSalary(salBracket,eSal))
						{
							System.out.println("Entered Salary is not in Salary Brackets: ["+salBracket[0]+"-"+salBracket[1]+"]:\n \n ");
						}
					}while(!adminService.isValidSalary(salBracket,eSal));
				}
			}while(!adminService.isValidGradeCode(validateGradeCode,eGradeCode));
			
			System.out.println("Enter Employee Designation:");
			String eDesig=sc.next();
			
			
			int gen;
			String eGender=null;
			do
			{
				System.out.println("Select Gender:\n 1. Male \n 2. Female  \n 3. Other");
				gen=sc.nextInt();
				if(!adminService.isValidGender(gen))
				{
					System.out.println("Entered Gender in not valid:\n \n");
				}
				else if(adminService.isValidGender(gen))
				{
				
					switch(gen)
					{
					case 1: eGender="M";
					break;
					case 2: eGender="F";
					break;
					case 3: eGender="O";
					break;
					}
				}				
			}while(!adminService.isValidGender(gen));
			
			int status;
			String eMarStatus=null;
			do
			{
				System.out.println("Select Maritial Status:\n 1. Single\n 2. Married\n 3. Divorced\n 4. Seperated\n 5. Widowed");
				status=sc.nextInt();
				if(!adminService.isValidMaritialStatus(status))
				{
					System.out.println("Entered Maritial Status in not valid:\n \n");
				}
				else if(adminService.isValidMaritialStatus(status))
				{

					
					switch(status)
					{
					case 1: eMarStatus="S";
					break;
					case 2: eMarStatus="M";
					break;
					case 3: eMarStatus="D";
					break;
					case 4: eMarStatus="S";
					break;
					case 5: eMarStatus="W";
					break;
					}
				}
			}while(!adminService.isValidMaritialStatus(status));
			
			System.out.println("Enter Employee Address:");
			String eAddr=sc.next();
			
			long eContact;
			do
			{
				System.out.println("Enter Employee Contact Number:");
				eContact=sc.nextLong();
				if(!adminService.isValidContact(eContact))
				{
					System.out.println("Invalid contact number, should be 10 digits:\n \n");
				}
			}while(!adminService.isValidContact(eContact));
			
			String eMngID;
			do
			{
				System.out.println("Enter Employee Manager ID:");
				eMngID=sc.next();
				if(!adminService.validateEmpId(eMngID))
				{
					System.out.println("Manager ID Should be 6 digits:");	
				}
			}while(!adminService.validateEmpId(eMngID));
			
			
			Employee emp=new Employee(eId,eFName,eLName,eDOB,eDOJ,eDepId,eGradeCode,eDesig,eSal,eGender,eMarStatus,eAddr,eContact,eMngID);
			if(adminService.addEmployee(emp)==1)
			{
				System.out.println("Employee added Successfully");
			}
			else
			{
				System.out.println("Insert Operation failed");
			}
		}
		
		catch(EmployeeException e)
		{
			System.out.println(e.getMessage());
		}
	
	}

	}


