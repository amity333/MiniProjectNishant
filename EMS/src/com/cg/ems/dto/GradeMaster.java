package com.cg.ems.dto;

public class GradeMaster {
	private String gradeCode;
	private int minSalary;
	private int maxSalary;
	
	public GradeMaster() {
		super();
	}
	public GradeMaster(String gradeCode, int minSalary, int maxSalary) {
		super();
		this.gradeCode = gradeCode;
		this.minSalary = minSalary;
		this.maxSalary = maxSalary;
	}
	public String getGradeCode() {
		return gradeCode;
	}
	public void setGradeCode(String gradeCode) {
		this.gradeCode = gradeCode;
	}
	public int getMinSalary() {
		return minSalary;
	}
	public void setMinSalary(int minSalary) {
		this.minSalary = minSalary;
	}
	public int getMaxSalary() {
		return maxSalary;
	}
	public void setMaxSalary(int maxSalary) {
		this.maxSalary = maxSalary;
	}
	@Override
	public String toString() {
		return "GradeMaster [gradeCode=" + gradeCode + ", minSalary=" + minSalary + ", maxSalary=" + maxSalary + "]";
	}
}
