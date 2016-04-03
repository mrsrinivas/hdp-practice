package com.practice.dateconverter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.practice.util.LogUtil;

public class DateParser {

	private static Logger LOGGER = LogUtil.getLoggerInstance(DateParser.class.getName());

	// Jan 11, 2003 6/17/1969 08/22/54 =>MM/DD/YY

	private static String[] formats = { "MMM dd, yyyy", "MM/dd/yyyy", "dd/MM/yy" };

	public Date parse(String dateStr) {

		Date resp = null;

		for (String format : formats) {
			try {
				resp = new SimpleDateFormat(format, Locale.ENGLISH).parse(dateStr);
				LOGGER.log(Level.FINE, "Successfully date (" + dateStr + ") parsed with " + format);
			} catch (ParseException e) {
				LOGGER.log(Level.WARNING, "Failed to parse date (" + dateStr + ") with " + format);
			} finally {
				if (resp != null) {
					break;
				}
			}
		}
		return resp;
	}

}