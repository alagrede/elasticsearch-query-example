package com.tony.el;

import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.http.HttpHost;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tony.el.ServiceELHighImpl;
import com.tony.el.domain.MessageIndex;

import junit.framework.Assert;

public class ServiceELHighImplTest {

	static ServiceELHighImpl provider; 
	static List<String> ids = new ArrayList<>();
	
	@BeforeClass public static void setUp() throws UnknownHostException {
	
		HttpHost host = new HttpHost("localhost", 9200, "http");
		  
		provider = new ServiceELHighImpl(Arrays.asList(host));
		
		ids.add(provider.putMetastore(arMessageRandomBuilder(yesterdayMidnight(), yesterday5h())));
		ids.add(provider.putMetastore(arMessageRandomBuilder(yesterday7h(), yesterday14h())));
		ids.add(provider.putMetastore(arMessageRandomBuilder(todayMidnight(), today5h())));
	}

	@AfterClass public static void tearDown() {
		provider.deleteMetastore(ids);
	}
	
	
	@Test public void searchByDateAll() {
		String[] refNumbers = {"refnumber"};
		String[] params = {"param1"};
		
		long startTime = yesterdayMidnight();
		long stopTime = twoDaysLater();

		List<MessageIndex> messages = provider.getFilesIndexed(refNumbers, startTime, stopTime, params);
		
		Assert.assertEquals(3, messages.size());
	}

	@Test public void searchByDateYesterday() {
		String[] refNumbers = {"refnumber"};
		String[] params = {"param1"};
		
		long startTime = yesterday1h();
		long stopTime = yesterday15h();

		List<MessageIndex> messages = provider.getFilesIndexed(refNumbers, startTime, stopTime, params);
		
		Assert.assertEquals(2, messages.size());
	}

	@Test public void searchByDateTodayPast() {
		String[] refNumbers = {"refnumber"};
		String[] params = {"param1"};
		
		long startTime = today5h();
		long stopTime = today6h();

		List<MessageIndex> messages = provider.getFilesIndexed(refNumbers, startTime, stopTime, params);
		
		Assert.assertEquals(1, messages.size());
	}
	
	@Test public void searchUnknownParam() {
		String[] refNumbers = {"refnumber"};
		String[] params = {"param5"}; //unknow param
		
		long startTime = yesterdayMidnight();
		long stopTime = twoDaysLater();

		List<MessageIndex> messages = provider.getFilesIndexed(refNumbers, startTime, stopTime, params);
		
		Assert.assertEquals(0, messages.size());
	}
	
	
	
	@Test public void searchNoParam() {
		String[] refNumbers = {"refnumber"};
		String[] params = {};
		
		long startTime = yesterdayMidnight();
		long stopTime = twoDaysLater();

		List<MessageIndex> messages = provider.getFilesIndexed(refNumbers, startTime, stopTime, params);
		
		Assert.assertEquals(3, messages.size());
	}


	private long today6h() {
		return Timestamp.valueOf(LocalDate.now().atStartOfDay().plusHours(6)).getTime();
	}
	private long twoDaysLater() {
		return Timestamp.valueOf(LocalDate.now().atStartOfDay().plusDays(2)).getTime();
	}

	private long yesterday15h() {
		return Timestamp.valueOf(LocalDate.now().atStartOfDay().minusDays(1).plusHours(15)).getTime();
	}

	private long yesterday1h() {
		return Timestamp.valueOf(LocalDate.now().atStartOfDay().minusDays(1).plusHours(1)).getTime();
	}

	private static long todayMidnight() {
		return Timestamp.valueOf(LocalDate.now().atStartOfDay()).getTime();
	}
	private static long today5h() {
		return Timestamp.valueOf(LocalDate.now().atStartOfDay().plusHours(5)).getTime();
	}

	private static long yesterdayMidnight() {
		return Timestamp.valueOf(LocalDate.now().atStartOfDay().minusDays(1)).getTime();
	}
	private static long yesterday5h() {
		return Timestamp.valueOf(LocalDate.now().atStartOfDay().minusDays(1).plusHours(5)).getTime();
	}
	private static long yesterday7h() {
		return Timestamp.valueOf(LocalDate.now().atStartOfDay().minusDays(1).plusHours(7)).getTime();
	}
	private static long yesterday14h() {
		return Timestamp.valueOf(LocalDate.now().atStartOfDay().minusDays(1).plusHours(14)).getTime();
	}

	private static MessageIndex arMessageRandomBuilder(long startDate, long stopDate) {
		String[] params = {"param1", "param2", "param3"};
		MessageIndex msg = new MessageIndex();
		msg.setDateStart(startDate);
		msg.setDateStop(stopDate);
		msg.setParameters(params);
		msg.setFileName("/tmp/fileName/file.csv");
		msg.setRefNumber("refnumber");
		msg.setUploadedDate(new Date());
		return msg;
	}
	
}
