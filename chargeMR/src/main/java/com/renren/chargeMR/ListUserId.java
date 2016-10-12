package com.renren.chargeMR;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class ListUserId {
	static List<String> listUserId = new LinkedList<String>(Arrays.asList(
			""));
	static public void addUserId(String uid) {
		listUserId.add(uid);
	}
	static public boolean isExist(String str) {
		return listUserId.contains(str);
	}
}
