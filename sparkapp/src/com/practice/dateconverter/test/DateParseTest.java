package com.practice.dateconverter.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Ignore;
import org.junit.Test;

import com.practice.dateconverter.DateParser;
import com.practice.util.LogUtil;

public class DateParseTest {

	private static Logger LOGGER = LogUtil.getLoggerInstance(DateParseTest.class.getName());

	// @Ignore
	@Test
	public void given_dates_when_parsed_then_return_date() {

		String[] dates = { "Jan 11, 2003", "6/17/1969", "08/22/54" };

		DateParser parser = new DateParser();

		for (String date : dates) {
			LOGGER.log(Level.INFO, "Date input => " + date);
			Date dateObj = parser.parse(date);
			if (dateObj != null) {
				LOGGER.log(Level.INFO, "Date output => " + dateObj);
				LOGGER.log(Level.INFO, "SimpleDateFormat(\"MM-dd-yyyy\").format(dateObj) => "
						+ new SimpleDateFormat("MM/dd/yy").format(dateObj));
			} else {
				LOGGER.log(Level.INFO, "Date output null ");
			}
		}
	}

	@Test
	@Ignore
	public void test() throws ParseException {
		String string = "January 2, 2010";
		Date date = new SimpleDateFormat("MMMM d, yyyy", Locale.ENGLISH).parse(string);
		System.out.println(date); //
	}

}
