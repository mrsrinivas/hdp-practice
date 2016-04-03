package com.practice.util;

import java.util.logging.Level;
import java.util.logging.Logger;

public class LogUtil extends Logger {

	protected LogUtil(String name, String resourceBundleName) {
		super(name, resourceBundleName);
	}

	public static Logger getLoggerInstance(String className) {
		return getLogger(className);
	}

	public void log(Level level, String msg) {
		log(level, Thread.currentThread() + " " + msg);
	}

}