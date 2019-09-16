package com.cg.ems.dto;

public class Department {
	private int deptID;
	private String deptName;
	public int getDeptID() {
		return deptID;
	}
	public void setDeptID(int deptID) {
		this.deptID = deptID;
	}
	public String getDeptName() {
		return deptName;
	}
	public void setDeptName(String deptName) {
		this.deptName = deptName;
	}
	public Department( String deptName,int deptID) {
		super();
		this.deptID = deptID;
		this.deptName = deptName;
	}
	@Override
	public String toString() {
		return "Department [deptID=" + deptID + ", deptName=" + deptName + "]";
	}
	
	
	/*private String empDesignation;

	private int empDeptID;
	public Department(String empDesignation, int empDeptID) {
		super();
		this.empDesignation = empDesignation;
		this.empDeptID = empDeptID;
	}
	public Department() {
		super();
	}
	public String getEmpDesignation() {
		return empDesignation;
	}
	public void setEmpDesignation(String empDesignation) {
		this.empDesignation = empDesignation;
	}
	public int getEmpDeptID() {
		return empDeptID;
	}
	public void setEmpDeptID(int empDeptID) {
		this.empDeptID = empDeptID;
	}
	@Override
	public String toString() 
	{
		System.out.println("Department ID\t\t Department Name");
		return  empDesignation + "\t\t" + empDeptID ;
	}
	*/
}
