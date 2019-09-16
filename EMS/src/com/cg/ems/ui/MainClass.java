
package com.cg.ems.ui;

public class MainClass 
{
	public static void main(String[] args) 
	{
		/*************************************************************
		 * This function should be called when Admin credentials are validated to access 
		 *  Admin functionality. 
		 * */
		adminFunctionality();
		
	}
	public static void adminFunctionality()
	{
		//AdminMainClass adminMain=new AdminMainClass();
		AdminMainClassV2 adminMain=new AdminMainClassV2();
		adminMain.adminMainClass();
		
	}
}
