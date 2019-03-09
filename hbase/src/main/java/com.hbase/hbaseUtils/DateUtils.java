package com.hbase.hbaseUtils;

import java.text.SimpleDateFormat;
import java.util.Date;
public class DateUtils
{
	public static String getTodayTableName()
	{
		String timePattern = "yyyyMMdd";
		String tableName = "" ;
		Date date = new Date();// �½���ʱ�ĵ�ϵͳʱ��
		SimpleDateFormat dateFormat = new SimpleDateFormat(timePattern);
		tableName =  dateFormat.format(date);
		return tableName ;
	}
	

	public void testDateAndTime()
	{
		String date = "2017-05-15"; 
		String time = "" ;
		System.out.println(DateUtils.dateAndTimeHandle(date, time));
	}
	
	public static String dateAndTimeHandle(String date,String time)
	{
		//System.out.println(date);
		String resultDateStr = "" ;
		String resultTimeStr = "";
		int year = 0 ;
		int month = 0 ;
		int day = 0 ;
		int hours = 0 ;
		int minute = 0 ;
		int second = 0 ;
		if(null != date && !date.isEmpty())
		{
			if (date.contains("-"))
			{
				date = date.replaceAll("-","");
				//System.out.println(date);
				date = date.substring(2, date.length());
				//System.out.println(date);
			}
			year = Integer.parseInt(date.substring(0, 2));
			year = 99 - year ;
			month = Integer.parseInt(date.substring(2, 4));
			month = 12 - month ;
			day = Integer.parseInt(date.substring(4, 6));
			day = 31 - day ;
			String yearStr = String.format("%02d", year);		
			String monthStr = String.format("%02d", month);		
			String dayStr = String.format("%02d", day);
			resultDateStr = yearStr + monthStr + dayStr ;
		}
		
		if(time!=null && !time.isEmpty())
		{
			if (time.contains(":"))
			{
				time = time.replaceAll(":","");
			}
			hours = Integer.parseInt(time.substring(0, 2));
			hours = 24 - hours ;
			minute = Integer.parseInt(time.substring(2, 4));
			minute = 60 - minute ;
			second = Integer.parseInt(time.substring(4, 6));
			second = 60 - second ;
			String hoursStr = String.format("%02d", year);		
			String minuteStr = String.format("%02d", month);		
			String secondStr = String.format("%02d", day);
			resultTimeStr = hoursStr + minuteStr + secondStr ;
		}
		else
		{
			resultTimeStr = "000000";
		}
		// 13:59:05 --> 135905
		// after replace is 20170515 sub is 170515 
		return resultDateStr + resultTimeStr ;
		
	}
	

}
