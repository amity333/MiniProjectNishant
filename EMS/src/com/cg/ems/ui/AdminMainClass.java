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

public class AdminMainClass 
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
		
		
	}

	private void addNewEmployee() 
	{
		try
		{
		System.out.println("Enter Employee ID:");
		String eId=sc.next();
		if(adminService.validateEmpId(eId))
		{
			System.out.println("Enter Employee First Name:");
			String eFName=sc.next();
			if(adminService.validateName(eFName, 1))
			{
				System.out.println("Enter Employee Last Name:");
				String eLName=sc.next();
				if(adminService.validateName(eLName, 2))
				{
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
					System.out.println("Select Department ID:");
					int eDepId=sc.nextInt();
					if(adminService.validateDeptID(validateDepId,eDepId))
					{
						//String eDepName=adminService.getDepartment(eDepId);
					
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
						String eGradeCode=sc.next();
						eGradeCode=eGradeCode.toUpperCase();
						if(adminService.isValidGradeCode(validateGradeCode,eGradeCode))
						{
							int[] salBracket=new int[2];
							salBracket=adminService.salaryBracket(eGradeCode);
							System.out.println("Enter Employee basic Salary:");
							int eSal=sc.nextInt();
							if(adminService.isValidSalary(salBracket,eSal))
							{
								System.out.println("Enter Employee Designation:");
								String eDesig=sc.next();
								System.out.println("Select Gender:\n 1. Male \n 2. Female  \n 3. Other");
								int gen=sc.nextInt();
								if(adminService.isValidGender(gen))
								{
									String eGender=null;
									switch(gen)
									{
									case 1: eGender="M";
									break;
									case 2: eGender="F";
									break;
									case 3: eGender="O";
									break;
									}
									System.out.println("Enter Maritial Status:");
									System.out.println(" 1. Single\n 2. Married\n 3. Divorced\n 4. Seperated\n 5. Widowed");
									int status=sc.nextInt();
									if(adminService.isValidMaritialStatus(status))
									{
										String eMarStatus=null;
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
										System.out.println("Enter Employee Address:");
										String eAddr=sc.next();
										System.out.println("Enter Employee Contact Number:");
										long eContact=sc.nextLong();
										if(adminService.isValidContact(eContact))
										{
											System.out.println("Enter Employee Manager ID:");
											String eMngID=sc.next();
											if(adminService.validateEmpId(eMngID))
											{
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
										}
									}
								}
								
							}
							
						}
					}
					
					
				    
				}
			}
		}
		}
		catch(EmployeeException e)
		{
			System.out.println(e.getMessage());
		}
	
	}


}
