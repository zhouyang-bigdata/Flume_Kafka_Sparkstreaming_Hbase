package com.hbase.hbaseUtils;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

public class DateUtil {
	public static final String timePattern = "yyyy-MM-dd HH:mm:ss";
	public static final String dtShort = "yyyyMMdd";
	public static final int rowkeyDDHHMMSS = 32246060;
	public static final long maxSecondTimde = 30000000000L;
	public static Long getCurrentSecondTime() {
		return Long.valueOf(System.currentTimeMillis() / 1000L);
	}

	/**
	 * 获取系统当期年月�?(精确到天)，格式：yyyyMMdd
	 * @return
	 */
	public static String getDate(){
		Date date=new Date();
		DateFormat df=new SimpleDateFormat(dtShort);
		return df.format(date);
	}
	
	public static Long getCurrentMsTime() {
		return Long.valueOf(System.currentTimeMillis());
	}

	/**
	 * 
	 * @return
	 */
	public static Long getCurrentMsTimeReverse() {
		return Long.valueOf(9999999999999L - System.currentTimeMillis());
	}

	/**
	 * 
	 * @return
	 */
	public static String getCurrentSecondTimeStr() {
		SimpleDateFormat df = new SimpleDateFormat(timePattern);
		return df.format(new Date());
	}

	public static String getDateTime(long secondTime) {
		return getDateTime(secondTime, timePattern);
	}

	public static String getDateTime(Date date) {
		SimpleDateFormat dateFormat = new SimpleDateFormat(timePattern);
		return dateFormat.format(date);
	}

	public static String getDateTime(String time) {
		if (time.contains("-") || time.contains("/"))
			return time;
		return getDateTime(Long.parseLong(time));
	}
	public static String getDateTime(long secondTime, String timePattern) {
		if (timePattern == null) {
			throw new IllegalArgumentException("time parttern is null");
		}
		SimpleDateFormat dateFormat = new SimpleDateFormat(timePattern);
		return dateFormat.format(new Date(secondTime * 1000L));
	}
	public static long getTotalSec(String time) {
		return getTotalSecMS(time) / 1000;
	}

	public static long getTotalSec(Date date) {
		return getTotalSecMS(date) / 1000L;
	}
	public static long getTotalSec(String time, String timePattern) {
		return getTotalSecMS(time, timePattern) / 1000L;
	}
	public static long getTotalSecMS(String time) {
		if (time.contains("-") || time.contains("/")) {
			if (time.toUpperCase().contains("T") || time.toUpperCase().contains("Z"))
				try {
					time = utc2Local(time);
				} catch (ParseException e) {
				}
			return getTotalSecMS(time, getTimePattern(time));
		} else {
			long t = Long.parseLong(time);
			return t > maxSecondTimde ? t : Long.parseLong(time) * 1000;
		}
	}

	/**
	 * ��ȡ���Ժ���ֵ
	 * 
	 * @param date
	 * @return
	 */
	public static long getTotalSecMS(Date date) {
		SimpleDateFormat dateFormat = new SimpleDateFormat(timePattern);
		String time = dateFormat.format(date);
		return getTotalSecMS(time);
	}

	/**
	 * ��ȡ���Ժ���ֵ
	 * 
	 * @param time
	 *            ����
	 * @param timePattern
	 * @return
	 */
	public static long getTotalSecMS(String time, String timePattern) {
		if (timePattern == null) {
			throw new IllegalArgumentException("time parttern is null");
		}
		if (time == null) {
			throw new IllegalArgumentException("time is null");
		}
		if (time.contains("-") || time.contains("/")) {
			SimpleDateFormat dateFormat = new SimpleDateFormat(timePattern);
			try {
				return dateFormat.parse(time).getTime();
			} catch (ParseException e) {
			}
		} else {
			long t = Long.parseLong(time);
			return t > maxSecondTimde ? t : Long.parseLong(time) * 1000;
		}
		throw new RuntimeException("Invalid formate");
	}

	/**
	 * ��ȡʱ���ʽ
	 * 
	 * @param time
	 * @return
	 */
	private static String getTimePattern(String time) {
		if (time == null) {
			throw new IllegalArgumentException("time is null");
		}
		String[] spl = time.split(" ");
		String timePattern = "";
		if (spl[0].contains("/"))
			timePattern = String.format("yyyy%sMM%sdd", "/", "/");
		else if (spl[0].contains("-"))
			timePattern = String.format("yyyy%sMM%sdd", "-", "-");
		if (spl != null && spl.length > 1) {
			timePattern = String.format("%s HH%smm%sss", timePattern, ":", ":");
			if (spl[1].contains("."))
				timePattern = String.format("%s.SSS", timePattern);
		}
		return timePattern;
	}

	/**
	 * ��ȡ��ʱ����
	 * 
	 * @param time
	 *            ʱ��
	 * @return
	 */
	public static String getDDHHMMSS(String time) {
		if (time.contains("-") || time.contains("/"))
			return getDDHHMMSS(getTotalSec(time));
		else
			return getDDHHMMSS(Long.parseLong(time));
	}

	/**
	 * ��ȡ��ʱ����
	 * 
	 * @param time
	 *            ʱ��
	 * @return
	 */
	public static String getDDHHMMSS(long time) {
		Calendar cal = new GregorianCalendar();
		cal.setTimeInMillis(time * 1000L);
		String DDHHMMSS = String.format("%02d%02d%02d%02d", cal.get(Calendar.DATE), cal.get(Calendar.HOUR_OF_DAY),
				cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND));
		return DDHHMMSS;
	}

	/**
	 * ��ʱ�併��
	 * 
	 * @param time
	 * @return
	 */
	public static String getRowkeyDDHHMMSS(String time) {
		int ddhhmmss = Integer.parseInt(getDDHHMMSS(time));
		return String.format("%08d", rowkeyDDHHMMSS - ddhhmmss);
	}

	/**
	 * ��ʱ�併��
	 * 
	 * @param time
	 * @return
	 */
	public static String getRowkeyDDHHMMSS(long time) {
		int ddhhmmss = Integer.parseInt(getDDHHMMSS(time));
		return String.format("%08d", rowkeyDDHHMMSS - ddhhmmss);
	}

	/**
	 * ��ʱ�併��
	 * 
	 * @param ddhhmmss
	 * @return
	 */
	public static String getRowKeyDDHHMMSSByDDHHMMSS(int ddhhmmss) {
		return String.format("%08d", rowkeyDDHHMMSS - ddhhmmss);
	}

	/**
	 * ��ȡ����
	 * 
	 * @param time
	 *            ʱ��
	 * @return
	 */
	public static String getYYMM(String time) {
		if (time.contains("-") || time.contains("/"))
			return getYYMM(getTotalSec(time));
		else
			return getYYMM(Long.parseLong(time));
	}

	/**
	 * ��ȡ����
	 * 
	 * @param time
	 * @return
	 */
	public static String getYYMM(long time) {
		Calendar cal = new GregorianCalendar();
		cal.setTimeInMillis(time * 1000L);
		String YYMM = String.format("%04d%02d", cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1);
		return YYMM;
	}

	/**
	 * ��ȡ������
	 * 
	 * @param time
	 *            ʱ��
	 * @return
	 */
	public static String getYYMMDD(String time) {
		if (time.contains("-") || time.contains("/"))
			return getYYMMDD(getTotalSec(time));
		else
			return getYYMMDD(Long.parseLong(time));
	}

	/**
	 * ��ȡ������
	 * 
	 * @param time
	 * @return
	 */
	public static String getYYMMDD(long time) {
		Calendar cal = new GregorianCalendar();
		cal.setTimeInMillis(time * 1000L);
		String YYMM = String.format("%04d%02d%02d", cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1,
				cal.get(Calendar.DATE));
		return YYMM;
	}

	/**
	 * ��ȡ����
	 * 
	 * @param begintTime
	 * @param endTime
	 * @return
	 */
	public static List<String> listYYMMBetweenLongDate(long begintTime, long endTime) {
		List<String> dataList = new ArrayList<String>();
		if (begintTime > 0 && begintTime <= endTime) {
			Calendar btime = getYYMMTime(begintTime);
			Calendar etime = getYYMMTime(endTime);
			while (btime.compareTo(etime) <= 0) {
				dataList.add(getYYMM(etime.getTimeInMillis() / 1000L));
				etime.add(Calendar.MONTH, -1);
			}
		}
		return dataList;
	}

	/**
	 * ��ȡ������
	 * 
	 * @param begintTime
	 * @param endTime
	 * @return
	 */
	public static List<String> listYYMMDDBetweenLongDate(long begintTime, long endTime) {
		List<String> dataList = new ArrayList<String>();
		if (begintTime > 0 && begintTime <= endTime) {
			Calendar btime = getYYMMDDTime(begintTime);
			Calendar etime = getYYMMDDTime(endTime);
			while (btime.compareTo(etime) <= 0) {
				dataList.add(getYYMMDD(etime.getTimeInMillis() / 1000L));
				etime.add(Calendar.DAY_OF_MONTH, -1);
			}
		}
		return dataList;
	}

	/**
	 * 
	 * @param time
	 * @return
	 */
	public static Calendar getYYMMTime(long time) {
		Calendar btime = new GregorianCalendar();
		btime.setTimeInMillis(time * 1000L);
		return new GregorianCalendar(btime.get(Calendar.YEAR), btime.get(Calendar.MONTH), 1);
	}

	/**
	 * 
	 * @param time
	 * @return
	 */
	public static Calendar getYYMMDDTime(long time) {
		Calendar btime = new GregorianCalendar();
		btime.setTimeInMillis(time * 1000L);
		return new GregorianCalendar(btime.get(Calendar.YEAR), btime.get(Calendar.MONTH),
				btime.get(Calendar.DAY_OF_MONTH));
	}

	/**
	 * ����ʱ��תUTC
	 * 
	 * @param time
	 * @return
	 */
	public static String local2Utc(String time) {
		return local2Utc(getTotalSec(time));
	}

	/**
	 * ����ʱ��תUTC
	 * 
	 * @param time
	 * @return
	 */
	public static String local2Utc(Long time) {
		long t = time * 1000; // + 8 * 60 * 60 * 1000;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		return sdf.format(t);
	}

	/**
	 * usת����ʱ��
	 * 
	 * @param time
	 * @return
	 * @throws ParseException
	 */
	public static String us2Local(String time) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", java.util.Locale.US);
		SimpleDateFormat sdf1 = new SimpleDateFormat(timePattern);
		return sdf1.format(sdf.parse(time));
	}

	/**
	 * utcת����ʱ��
	 * 
	 * @param time
	 * @return
	 * @throws ParseException
	 */
	public static String utc2Local(String time) throws ParseException {
		String pattern = time.contains(".") ? "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" : "yyyy-MM-dd'T'HH:mm:ss'Z'";
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		SimpleDateFormat sdf1 = new SimpleDateFormat(timePattern);
		// sdf1.setTimeZone(TimeZone.getDefault());
		Date d = sdf.parse(time);
		return sdf1.format(d);
	}

	public static int compare(Date dt1, Date dt2) {
		if (dt1.getTime() > dt2.getTime()) {
			return 1;
		} else if (dt1.getTime() < dt2.getTime()) {
			return -1;
		} else {
			return 0;
		}
	}
}
