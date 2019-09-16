package com.cg.ems.dto;

import java.time.LocalDate;

public class LeaveRecords {
	private int LeaveId;
	private String empId;
	private String empName;
	private LocalDate toDate;
	private LocalDate fromDate;
	private int noOfDays;
	private String status;
	private int leaveBalance;
	public LeaveRecords(int leaveId, String empId, String empName, LocalDate toDate, LocalDate fromDate, int noOfDays,
			String status, int leaveBalance) {
		super();
		LeaveId = leaveId;
		this.empId = empId;
		this.empName = empName;
		this.toDate = toDate;
		this.fromDate = fromDate;
		this.noOfDays = noOfDays;
		this.status = status;
		this.leaveBalance = leaveBalance;
	}
	public LeaveRecords() {
		super();
	}
	public int getLeaveId() {
		return LeaveId;
	}
	public void setLeaveId(int leaveId) {
		LeaveId = leaveId;
	}
	public String getEmpId() {
		return empId;
	}
	public void setEmpId(String empId) {
		this.empId = empId;
	}
	public String getEmpName() {
		return empName;
	}
	public void setEmpName(String empName) {
		this.empName = empName;
	}
	public LocalDate getToDate() {
		return toDate;
	}
	public void setToDate(LocalDate toDate) {
		this.toDate = toDate;
	}
	public LocalDate getFromDate() {
		return fromDate;
	}
	public void setFromDate(LocalDate fromDate) {
		this.fromDate = fromDate;
	}
	public int getNoOfDays() {
		return noOfDays;
	}
	public void setNoOfDays(int noOfDays) {
		this.noOfDays = noOfDays;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public int getLeaveBalance() {
		return leaveBalance;
	}
	public void setLeaveBalance(int leaveBalance) {
		this.leaveBalance = leaveBalance;
	}
	@Override
	public String toString() {
		return "LeaveRecords [LeaveId=" + LeaveId + ", empId=" + empId + ", empName=" + empName + ", toDate=" + toDate
				+ ", fromDate=" + fromDate + ", noOfDays=" + noOfDays + ", status=" + status + ", leaveBalance="
				+ leaveBalance + "]";
	}
}
