package com.cg.ems.dto;

import java.time.LocalDate;

public class Employee {
	private String empID;
	private String empFirstName;
	private String empLastName;
	private LocalDate empDateofBirth;
	private LocalDate empDateofJoining;
	//private String empDepartment;
	private String empDesignation;
	private int empDepartmentId;
	private String empGrade;
	private int empBasicSal;
	private String empGender;
	private String empMaritalStatus;
	private String empHomeAddress;
	private long empContactNum;
	private String mgrId;
	
	
	public Employee(String empID, String empFirstName, String empLastName, LocalDate empDateofBirth,
			LocalDate empDateofJoining,  int empDepartmentId, String empGrade,String empDesignation, int empBasicSal,
			String empGender, String empMaritalStatus, String empHomeAddress, long empContactNum, String mgrId) {
		super();
		this.empID = empID;
		this.empFirstName = empFirstName;
		this.empLastName = empLastName;
		this.empDateofBirth = empDateofBirth;
		this.empDateofJoining = empDateofJoining;
		this.empDesignation = empDesignation;
		this.empDepartmentId = empDepartmentId;
		this.empGrade = empGrade;
		this.empBasicSal = empBasicSal;
		this.empGender = empGender;
		this.empMaritalStatus = empMaritalStatus;
		this.empHomeAddress = empHomeAddress;
		this.empContactNum = empContactNum;
		this.mgrId = mgrId;
	}
	

	public Employee(String empID, String empFirstName, String empLastName, String empDesignation, int empDepartmentId,
			String empGrade, int empBasicSal, String empGender, String empMaritalStatus, String empHomeAddress,
			long empContactNum) {
		super();
		this.empID = empID;
		this.empFirstName = empFirstName;
		this.empLastName = empLastName;
		this.empDesignation = empDesignation;
		this.empDepartmentId = empDepartmentId;
		this.empGrade = empGrade;
		this.empBasicSal = empBasicSal;
		this.empGender = empGender;
		this.empMaritalStatus = empMaritalStatus;
		this.empHomeAddress = empHomeAddress;
		this.empContactNum = empContactNum;
	}


	public Employee() 
	{
		super();
	}

	public Employee(String id) {
		// TODO Auto-generated constructor stub
		this.empID=id;
	}

	public String getEmpID() {
		return empID;
	}

	public void setEmpID(String empID) {
		this.empID = empID;
	}

	public String getEmpFirstName() {
		return empFirstName;
	}

	public void setEmpFirstName(String empFirstName) {
		this.empFirstName = empFirstName;
	}

	public String getEmpLastName() {
		return empLastName;
	}

	public void setEmpLastName(String empLastName) {
		this.empLastName = empLastName;
	}

	public LocalDate getEmpDateofBirth() {
		return empDateofBirth;
	}

	public void setEmpDateofBirth(LocalDate empDateofBirth) {
		this.empDateofBirth = empDateofBirth;
	}

	public LocalDate getEmpDateofJoining() {
		return empDateofJoining;
	}

	public void setEmpDateofJoining(LocalDate empDateofJoining) {
		this.empDateofJoining = empDateofJoining;
	}

	public String getEmpDesignation() {
		return empDesignation;
	}

	public void setEmpDesignation(String empDesignation) {
		this.empDesignation = empDesignation;
	}

	public int getEmpDepartmentId() {
		return empDepartmentId;
	}

	public void setEmpDepartmentId(int empDepartmentId) {
		this.empDepartmentId = empDepartmentId;
	}

	public String getEmpGrade() {
		return empGrade;
	}

	public void setEmpGrade(String empGrade) {
		this.empGrade = empGrade;
	}

	public int getEmpBasicSal() {
		return empBasicSal;
	}

	public void setEmpBasicSal(int empBasicSal) {
		this.empBasicSal = empBasicSal;
	}

	public String getEmpGender() {
		return empGender;
	}

	public void setEmpGender(String empGender) {
		this.empGender = empGender;
	}

	public String getEmpMaritalStatus() {
		return empMaritalStatus;
	}

	public void setEmpMaritalStatus(String empMaritalStatus) {
		this.empMaritalStatus = empMaritalStatus;
	}

	public String getEmpHomeAddress() {
		return empHomeAddress;
	}

	public void setEmpHomeAddress(String empHomeAddress) {
		this.empHomeAddress = empHomeAddress;
	}

	public long getEmpContactNum() {
		return empContactNum;
	}

	public void setEmpContactNum(long empContactNum) {
		this.empContactNum = empContactNum;
	}

	public String getMgrId() {
		return mgrId;
	}

	public void setMgrId(String mgrId) {
		this.mgrId = mgrId;
	}

	@Override
	public String toString() {
		return "Employee [empID=" + empID + ", empFirstName=" + empFirstName + ", empLastName=" + empLastName
				+ ", empDateofBirth=" + empDateofBirth + ", empDateofJoining=" + empDateofJoining + ", empDesignation="
				+ empDesignation + ", empDepartmentId=" + empDepartmentId + ", empGrade=" + empGrade + ", empBasicSal="
				+ empBasicSal + ", empGender=" + empGender + ", empMaritalStatus=" + empMaritalStatus
				+ ", empHomeAddress=" + empHomeAddress + ", empContactNum=" + empContactNum + ", mgrId=" + mgrId + "]";
	}
	
	
}
