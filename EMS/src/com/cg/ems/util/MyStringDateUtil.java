package com.cg.ems.util;

import java.sql.Date;
import java.text.DateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;

public class MyStringDateUtil 
{
	public static LocalDate fromSqlToLocalDate(Date sqlDate)
	{		
		LocalDate dobLocalDate=sqlDate.toLocalDate();
		return  dobLocalDate;
	}
	
	public static ArrayList<String> fromStringToArrayList(String str)
	{
		ArrayList<String> stringList=new ArrayList();
		for(String tempStr:str.split(","))
		{
			stringList.add(tempStr);
		}
		return stringList;
	}	
	
	public  static String[]  fromArrayListToArray(ArrayList<String> strList)
	{		
		System.out.println("************From Array List To String Array ****");
		
		String stringArray[]=strList.toArray
				(new String[strList.size()]);
		
		for(int i=0;i<stringArray.length;i++)
		{
			System.out.println(stringArray[i]);
		}
		return stringArray;
	}
	
	public static String fromArrayToCommaSeparatedString(String strArray[])
	{
		System.out.println("**********From Array String**********");
		StringBuffer str=new StringBuffer();
		for (String tempStr:strArray )
		{
			str.append(tempStr+",");
			//str+=tempStr+",";			
		}
		return str.toString();
	}
	
	public static Date fromLocalToSqlDate(LocalDate localDate)
	{		
		System.out.println(" from local to sql.....................");
		java.sql.Date sqlDate=java.sql.Date.valueOf(localDate);
		return sqlDate;

	}
	
	public static  LocalDate fromStringToLocalDate(String dobText)
	{
		LocalDate localDob=LocalDate.
				parse(dobText, 
						DateTimeFormatter.ofPattern("dd-MM-yyyy"));
				
		return localDob;
		
	}
	
	public static Date fromStringToSqlDate(String dobText)
	{
		LocalDate localDob=LocalDate.
				parse(dobText, 
						DateTimeFormatter.ofPattern("dd-MM-yyyy"));
		java.sql.Date sqlDate=java.sql.Date.valueOf(localDob);
		return sqlDate;
		
	}

}
