package com.hbase.hbaseUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
public class StringUtil {
	private static final String EMPTY_STRING = "";

	public static boolean isNullOrEmpty(String str) {
		return str == null || str.isEmpty();
	}

	public static final String getEmptyString() {
		return EMPTY_STRING;
	}

	public static int getInt(String str) {
		if (!isNullOrEmpty(str))
			return Integer.parseInt(str);
		return 0;
	}

	public static String filterString(String str) {
		if (isNullOrEmpty(str))
			return str;
		if (!str.toLowerCase().startsWith("http:"))
			str = str.replace("*", "").replace("[", "").replace("]", "").replace("{", "").replace("}", "")// .replace(",",
																											// "")
					.replace("\"", "").replace(":", "").replace("-", "");
		return str;
	}

	public static String padRight(String str, int len, String c) {
		if (isNullOrEmpty(str))
			str = "";
		int strlen = str.length();
		if (strlen < len) {
			for (int i = 0; i < len - strlen; i++) {
				str = str + c;
			}
		}
		return str;
	}

	public static String padLeft(String str, int len, String c) {
		if (isNullOrEmpty(str))
			str = "";
		int strlen = str.length();
		if (strlen < len) {
			for (int i = 0; i < len - strlen; i++) {
				str = c + str;
			}
		}
		return str;
	}

	public static final <T> String constructString(T[] strs, char seperator, StringBuilder sb) {
		if (strs == null) {
			throw new IllegalArgumentException("strs is null");
		}
		if (sb == null) {
			throw new IllegalArgumentException("sb is null");
		}

		sb.delete(0, sb.length());
		for (int i = 0; i < strs.length; i++) {
			if (i != 0) {
				sb.append(seperator);
			}

			sb.append(strs[i]);
		}
		return sb.toString();
	}

	public static final <T> String constructString(T[] strs, char seperator) {
		if (strs == null) {
			throw new IllegalArgumentException("strs is null");
		}

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < strs.length; i++) {
			if (i != 0) {
				sb.append(seperator);
			}

			sb.append(strs[i]);
		}
		return sb.toString();
	}

	public static final <T> String constructString(Collection<T> strs, char seperator) {
		if (strs == null) {
			throw new IllegalArgumentException("strs is null");
		}

		StringBuilder sb = new StringBuilder();

		boolean first = true;
		for (Iterator<T> i$ = strs.iterator(); i$.hasNext();) {
			Object t = i$.next();

			if (first) {
				first = false;
			} else {
				sb.append(seperator);
			}

			sb.append(t);
		}
		return sb.toString();
	}

	public static final <T> String constructString(List<T> strs, char seperator, StringBuilder sb) {
		if (strs == null) {
			throw new IllegalArgumentException("strs is null");
		}
		if (sb == null) {
			throw new IllegalArgumentException("sb is null");
		}

		sb.delete(0, sb.length());

		boolean first = true;
		for (Iterator<T> i$ = strs.iterator(); i$.hasNext();) {
			Object t = i$.next();

			if (first) {
				first = false;
			} else {
				sb.append(seperator);
			}

			sb.append(t);
		}
		return sb.toString();
	}

	/**
	 * 
	 * @param json
	 * @return
	 */
	public static Map<String, String> jsonToMap(String json) {
		Map<String, String> map = new HashMap<String, String>();
		if (StringUtil.isNullOrEmpty(json))
			return map;

		json = json.trim().replace("\"", "");
		if (json.startsWith("{"))
			json = json.substring(1);
		if (json.endsWith("}"))
			json = json.substring(0, json.length() - 1);

		// String[] spl1 = json.split(",");
		// for (String d : spl1) {
		// int idx = d.indexOf(":");
		// map.put(d.substring(0, idx), d.substring(idx + 1));
		// }

		return jsonSplit(json);
	}

	/**
	 * 
	 * @param json
	 * @return
	 */
	private static Map<String, String> jsonSplit(String json) {
		Map<String, String> map = new HashMap<String, String>();
		int idx = json.indexOf(":");
		while (idx != -1) {
			String key = json.substring(0, idx);
			json = json.substring(idx + 1);
			if (isNullOrEmpty(key)) {
				idx = json.indexOf(":");
				continue;
			}
			if (isNullOrEmpty(json))
				break;
			List<String> retList = getjsonValue(json, key);
			json = retList.get(1);
			idx = json.indexOf(":");
			map.put(key, retList.get(0));
		}
		return map;
	}

	/**
	 * 
	 * @param json
	 * @param key
	 * @return
	 */
	private static List<String> getjsonValue(String json, String key) {
		String value = json;
		int idx1 = -1;
		if (key.toLowerCase().contains("time"))
			idx1 = json.indexOf(",");
		else if (json.contains(":"))
			idx1 = json.indexOf(":");
		if (idx1 != -1) {
			value = json.substring(0, idx1);
			int idx2 = value.lastIndexOf(",");
			if (idx2 != -1) {
				value = value.substring(0, idx2);
				json = json.substring(idx2 + 1);
			} else
				json = json.substring(idx1 + 1);
		}
		List<String> retList = new ArrayList<String>();
		retList.add(value);
		retList.add(json);
		return retList;
	}

}
