package com.linkedin.databus.test.bootstrap;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;
import org.junit.Before;
import org.junit.Test;

import com.linkedin.databus.bootstrap.api.BootstrapProducerStatus;
import com.linkedin.databus.bootstrap.common.BootstrapCleanerConfig;
import com.linkedin.databus.bootstrap.common.BootstrapConfig;
import com.linkedin.databus.bootstrap.common.BootstrapConn;
import com.linkedin.databus.bootstrap.common.BootstrapDBCleaner;
import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO;
import com.linkedin.databus.bootstrap.common.BootstrapCleanerStaticConfig.BootstrapDBType;
import com.linkedin.databus.bootstrap.common.BootstrapCleanerStaticConfig.RetentionType;
import com.linkedin.databus.bootstrap.common.BootstrapProducerThreadBase;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventInfo;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus.core.KeyTypeNotImplementedException;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.test.TestUtil;
import com.linkedin.databus2.util.DBHelper;

public class TestBootstrapDBCleaner {
	public static final String MODULE = TestBootstrapDBCleaner.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

	private BootstrapConfig _config = null;
	private BootstrapDBMetaDataDAO _dao = null;
	private BootstrapCleanerConfig _cleanerConfig = null;
	private ByteArrayInputStream _bufStream = null;
	private ByteArrayInputStream _bufStream2 = null;
	private ByteArrayInputStream _bufStream3 = null;
	private ByteBuffer _buf = null;
	private int _srcId = 1;
	private String _srcName = "src1";

	@Before
	public void setup() {
		System.out.println("setup() called !!");

		BootstrapConn conn = new BootstrapConn();
		try {
			TestUtil.setupLogging(true, null, Level.INFO);
			InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());

			byte[] b = new byte[100000];
			_buf = ByteBuffer.wrap(b);
			_bufStream = new ByteArrayInputStream(b);
			_bufStream2 = new ByteArrayInputStream(b);
			_bufStream3 = new ByteArrayInputStream(b);

			_config = new BootstrapConfig();
			_config.setBootstrapDBHostname("localhost");
			_config.setBootstrapDBName("bootstrap");
			_config.setBootstrapDBUsername("bootstrap");
			_config.setBootstrapDBPassword("bootstrap");

			_cleanerConfig = new BootstrapCleanerConfig();

			BootstrapDBMetaDataDAO.createDB(_config.getBootstrapDBName(),
					_config.getBootstrapDBUsername(),
					_config.getBootstrapDBPassword(),
					_config.getBootstrapDBHostname());

			_dao = new BootstrapDBMetaDataDAO(conn,
					_config.getBootstrapDBHostname(),
					_config.getBootstrapDBUsername(),
					_config.getBootstrapDBPassword(),
					_config.getBootstrapDBName(), false);

			conn.initBootstrapConn(false, _config.getBootstrapDBUsername(),
					_config.getBootstrapDBPassword(),
					_config.getBootstrapDBHostname(),
					_config.getBootstrapDBName());

			_dao.reinitDB();

			_dao.addNewSourceInDB(_srcId, _srcName,
					BootstrapProducerStatus.ACTIVE);
		} catch (Exception ex) {
			LOG.error("Got exception while creating bootstrap connection", ex);
		}
	}

	@Test
	public synchronized void testTableBasedCleaneronFullBootstrapDB1() {
		System.out.println("testTableBasedCleaneronFullBootstrapDB1 !!");

		// Case when applier is at -1
		{
			// Create 10 log tables
			List<Long> seqs = new ArrayList<Long>();

			for (long i = 0; i < 900; i++)
				seqs.add(i);

			List<String> allKeyScns = null;

			try {
				allKeyScns = populateBootstrapDB(100, -1, seqs, null);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			BootstrapCleanerConfig cleanerConfig = new BootstrapCleanerConfig();
			cleanerConfig
					.setBootstrapTypeDefault(BootstrapDBType.BOOTSTRAP_FULL
							.toString());
			cleanerConfig.setEnableOptimizeTableDefault(false);
			cleanerConfig.setEnable(true);
			cleanerConfig.getRetentionDefault().setRetentionQuantity(5);
			cleanerConfig.getRetentionDefault().setRetentionType(
					RetentionType.RETENTION_LOGS.toString());
			List<String> sources = new ArrayList<String>();
			sources.add(_srcName);
			TestApplier applier = new TestApplier("testApplier");
			BootstrapDBCleaner cleaner = null;
			try {
				applier.start();
				cleaner = new BootstrapDBCleaner("testCleaner",
						cleanerConfig.build(), _config.build(), applier,
						sources);
				cleaner.doClean();
				List<String> gotLogFromLogInfo = getRemainingLogsFromLogInfo(_srcId);
				List<String> gotLogTables = getRemainingLogTables(_srcId);
				List<String> expectedLogs = new ArrayList<String>();
				for (int i = 0; i <= 9; i++)
					expectedLogs.add("log_" + _srcId + "_" + i);

				assertEquals("Remaining Log tables check 1", expectedLogs,
						gotLogFromLogInfo);
				assertEquals("Remaining Log tables check 2", expectedLogs,
						gotLogTables);

				List<String> gotTabRows = getScnKeyFromTable("tab_" + _srcId);
				assertEquals("Tab should be empty", 0, gotTabRows.size());

				List<String> expLogRows = new ArrayList<String>();
				for (int i = 0; i < allKeyScns.size(); i++) {
					expLogRows.add(allKeyScns.get(i));
				}

				List<String> gotLogRows = new ArrayList<String>();
				for (String l : gotLogFromLogInfo) {
					gotLogRows.addAll(getScnKeyFromTable(l));
				}

				assertEquals("Log Rows Check", expLogRows, gotLogRows);
				long gotStartScn = _dao.getBootstrapConn()
						.executeQueryAndGetLong(
								"select logstartscn from bootstrap_sources where id = "
										+ _srcId, -1);
				assertEquals("Expected StartSCN", 0L, gotStartScn);
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				if ((null != applier) && (applier.isAlive()))
					applier.shutdown();

				if (null != cleaner)
					cleaner.close();

				if (null != _dao)
					_dao.close();
			}
		}
	}

	@Test
	public synchronized void testTableBasedCleaneronFullBootstrapDB2() {
		System.out.println("testTableBasedCleaneronFullBootstrapDB2 !!");

		// Case when applier is NOT at -1 and Optimize is disabled
		{
			// Create 10 log tables
			List<Long> seqs = new ArrayList<Long>();

			for (long i = 0; i < 900; i++)
				seqs.add(i);

			List<String> allKeyScns = null;

			try {
				allKeyScns = populateBootstrapDB(100, 605, seqs, null);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			BootstrapCleanerConfig cleanerConfig = new BootstrapCleanerConfig();
			cleanerConfig
					.setBootstrapTypeDefault(BootstrapDBType.BOOTSTRAP_FULL
							.toString());
			cleanerConfig.setEnable(true);
			cleanerConfig.getRetentionDefault().setRetentionQuantity(5);
			cleanerConfig.getRetentionDefault().setRetentionType(
					RetentionType.RETENTION_LOGS.toString());
			List<String> sources = new ArrayList<String>();
			sources.add(_srcName);
			TestApplier applier = new TestApplier("testApplier");
			BootstrapDBCleaner cleaner = null;

			try {
				applier.start();
				cleaner = new BootstrapDBCleaner("testCleaner",
						cleanerConfig.build(), _config.build(), applier,
						sources);
				cleaner.doClean();
				List<String> gotLogFromLogInfo = getRemainingLogsFromLogInfo(_srcId);
				List<String> gotLogTables = getRemainingLogTables(_srcId);
				List<String> expectedLogs = new ArrayList<String>();
				for (int i = 1; i <= 9; i++)
					expectedLogs.add("log_" + _srcId + "_" + i);
				assertEquals("Remaining Log tables check 1", expectedLogs,
						gotLogFromLogInfo);
				assertEquals("Remaining Log tables check 2", expectedLogs,
						gotLogTables);

				List<String> expTabRows = new ArrayList<String>();
				for (int i = 0; i < 605; i++) {
					expTabRows.add(allKeyScns.get(i));
				}
				List<String> gotTabRows = getScnKeyFromTable("tab_" + _srcId);
				assertEquals("Tab Rows Check", expTabRows, gotTabRows);

				List<String> expLogRows = new ArrayList<String>();
				for (int i = 99; i < allKeyScns.size(); i++) {
					expLogRows.add(allKeyScns.get(i));
				}

				List<String> gotLogRows = new ArrayList<String>();
				for (String l : gotLogFromLogInfo) {
					gotLogRows.addAll(getScnKeyFromTable(l));
				}

				assertEquals("Log Rows Check", expLogRows, gotLogRows);
				long gotStartScn = _dao.getBootstrapConn()
						.executeQueryAndGetLong(
								"select logstartscn from bootstrap_sources where id = "
										+ _srcId, -1);
				assertEquals("Expected StartSCN", 99L, gotStartScn);
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				if ((null != applier) && (applier.isAlive()))
					applier.shutdown();

				if (null != cleaner)
					cleaner.close();

				if (null != _dao)
					_dao.close();
			}
		}
	}

	@Test
	public synchronized void testTimeBasedCleaneronFullBootstrapDB1() {
		System.out.println("testTableBasedCleaneronFullBootstrapDB1 !!");

		// Case when applier is at -1
		{
			List<Long> seqs = new ArrayList<Long>();
			long currTime = System.currentTimeMillis() * 1000000L;
			final long stepTime = 60 * 60 * 1000L * 1000000L; // 1 hr
			List<Long> timestamps = new ArrayList<Long>();
			for (long i = 0; i < 900; i++) {
				seqs.add(i);
				long t = currTime - ((900 - i) * stepTime);
				timestamps.add(t);
			}

			List<String> allKeyScns = null;

			try {
				allKeyScns = populateBootstrapDB(100, -1, seqs, timestamps);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			BootstrapCleanerConfig cleanerConfig = new BootstrapCleanerConfig();
			cleanerConfig
					.setBootstrapTypeDefault(BootstrapDBType.BOOTSTRAP_FULL
							.toString());
			cleanerConfig.setEnableOptimizeTableDefault(false);
			cleanerConfig.setEnable(true);
			cleanerConfig.getRetentionDefault().setRetentionQuantity(
					(currTime - timestamps.get(400)) / 1000000000L);
			cleanerConfig.getRetentionDefault().setRetentionType(
					RetentionType.RETENTION_SECONDS.toString());
			List<String> sources = new ArrayList<String>();
			sources.add(_srcName);
			TestApplier applier = new TestApplier("testApplier");
			BootstrapDBCleaner cleaner = null;
			try {
				applier.start();
				cleaner = new BootstrapDBCleaner("testCleaner",
						cleanerConfig.build(), _config.build(), applier,
						sources);
				cleaner.doClean();
				List<String> gotLogFromLogInfo = getRemainingLogsFromLogInfo(_srcId);
				List<String> gotLogTables = getRemainingLogTables(_srcId);
				List<String> expectedLogs = new ArrayList<String>();
				for (int i = 0; i <= 9; i++)
					expectedLogs.add("log_" + _srcId + "_" + i);

				assertEquals("Remaining Log tables check 1", expectedLogs,
						gotLogFromLogInfo);
				assertEquals("Remaining Log tables check 2", expectedLogs,
						gotLogTables);

				List<String> gotTabRows = getScnKeyFromTable("tab_" + _srcId);
				assertEquals("Tab should be empty", 0, gotTabRows.size());

				List<String> expLogRows = new ArrayList<String>();
				for (int i = 0; i < allKeyScns.size(); i++) {
					expLogRows.add(allKeyScns.get(i));
				}

				List<String> gotLogRows = new ArrayList<String>();
				for (String l : gotLogFromLogInfo) {
					gotLogRows.addAll(getScnKeyFromTable(l));
				}

				assertEquals("Log Rows Check", expLogRows, gotLogRows);
				long gotStartScn = _dao.getBootstrapConn()
						.executeQueryAndGetLong(
								"select logstartscn from bootstrap_sources where id = "
										+ _srcId, -1);
				assertEquals("Expected StartSCN", 0L, gotStartScn);
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				if ((null != applier) && (applier.isAlive()))
					applier.shutdown();

				if (null != cleaner)
					cleaner.close();

				if (null != _dao)
					_dao.close();
			}
		}
	}

	@Test
	public synchronized void testTimeBasedCleaneronFullBootstrapDB2() {
		System.out.println("testTableBasedCleaneronFullBootstrapDB2 !!");

		// Case when applier is NOT at -1 and applier is greater than the time
		// threshold
		{
			List<Long> seqs = new ArrayList<Long>();
			long currTime = System.currentTimeMillis() * 1000000L;
			final long stepTime = 60 * 60 * 1000L * 1000000L; // 1 hr
			List<Long> timestamps = new ArrayList<Long>();
			for (long i = 0; i < 900; i++) {
				seqs.add(i);
				long t = currTime - ((900 - i) * stepTime);
				timestamps.add(t);
			}

			List<String> allKeyScns = null;

			try {
				allKeyScns = populateBootstrapDB(100, 605, seqs, timestamps);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			BootstrapCleanerConfig cleanerConfig = new BootstrapCleanerConfig();
			cleanerConfig
					.setBootstrapTypeDefault(BootstrapDBType.BOOTSTRAP_FULL
							.toString());
			cleanerConfig.setEnable(true);
			cleanerConfig.getRetentionDefault().setRetentionQuantity(
					(currTime - timestamps.get(400)) / 1000000000L);
			cleanerConfig.getRetentionDefault().setRetentionType(
					RetentionType.RETENTION_SECONDS.toString());
			List<String> sources = new ArrayList<String>();
			sources.add(_srcName);
			TestApplier applier = new TestApplier("testApplier");
			BootstrapDBCleaner cleaner = null;

			try {
				applier.start();
				cleaner = new BootstrapDBCleaner("testCleaner",
						cleanerConfig.build(), _config.build(), applier,
						sources);
				cleaner.doClean();
				List<String> gotLogFromLogInfo = getRemainingLogsFromLogInfo(_srcId);
				List<String> gotLogTables = getRemainingLogTables(_srcId);
				List<String> expectedLogs = new ArrayList<String>();
				for (int i = 4; i <= 9; i++)
					expectedLogs.add("log_" + _srcId + "_" + i);
				assertEquals("Remaining Log tables check 1", expectedLogs,
						gotLogFromLogInfo);
				assertEquals("Remaining Log tables check 2", expectedLogs,
						gotLogTables);

				List<String> expTabRows = new ArrayList<String>();
				for (int i = 0; i < 605; i++) {
					expTabRows.add(allKeyScns.get(i));
				}
				List<String> gotTabRows = getScnKeyFromTable("tab_" + _srcId);
				assertEquals("Tab Rows Check", expTabRows, gotTabRows);

				List<String> expLogRows = new ArrayList<String>();
				for (int i = 399; i < allKeyScns.size(); i++) {
					expLogRows.add(allKeyScns.get(i));
				}

				List<String> gotLogRows = new ArrayList<String>();
				for (String l : gotLogFromLogInfo) {
					gotLogRows.addAll(getScnKeyFromTable(l));
				}

				assertEquals("Log Rows Check", expLogRows, gotLogRows);
				long gotStartScn = _dao.getBootstrapConn()
						.executeQueryAndGetLong(
								"select logstartscn from bootstrap_sources where id = "
										+ _srcId, -1);
				assertEquals("Expected StartSCN", 399L, gotStartScn);
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				if ((null != applier) && (applier.isAlive()))
					applier.shutdown();

				if (null != cleaner)
					cleaner.close();

				if (null != _dao)
					_dao.close();
			}
		}
	}

	@Test
	public synchronized void testTimeBasedCleaneronFullBootstrapDB3() {
		System.out.println("testTableBasedCleaneronFullBootstrapDB3 !!");

		// Case when applier is NOT at -1 and applier is less than the time
		// threshold
		{
			List<Long> seqs = new ArrayList<Long>();
			long currTime = System.currentTimeMillis() * 1000000L;
			final long stepTime = 60 * 60 * 1000L * 1000000L; // 1 hr
			List<Long> timestamps = new ArrayList<Long>();
			for (long i = 0; i < 900; i++) {
				seqs.add(i);
				long t = currTime - ((900 - i) * stepTime);
				timestamps.add(t);
			}

			List<String> allKeyScns = null;

			try {
				allKeyScns = populateBootstrapDB(100, 405, seqs, timestamps);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			BootstrapCleanerConfig cleanerConfig = new BootstrapCleanerConfig();
			cleanerConfig
					.setBootstrapTypeDefault(BootstrapDBType.BOOTSTRAP_FULL
							.toString());
			cleanerConfig.setEnable(true);
			cleanerConfig.getRetentionDefault().setRetentionQuantity(
					(currTime - timestamps.get(800)) / 1000000000L);
			cleanerConfig.getRetentionDefault().setRetentionType(
					RetentionType.RETENTION_SECONDS.toString());
			List<String> sources = new ArrayList<String>();
			sources.add(_srcName);
			TestApplier applier = new TestApplier("testApplier");
			BootstrapDBCleaner cleaner = null;

			try {
				applier.start();
				cleaner = new BootstrapDBCleaner("testCleaner",
						cleanerConfig.build(), _config.build(), applier,
						sources);
				cleaner.doClean();
				List<String> gotLogFromLogInfo = getRemainingLogsFromLogInfo(_srcId);
				List<String> gotLogTables = getRemainingLogTables(_srcId);
				List<String> expectedLogs = new ArrayList<String>();
				for (int i = 4; i <= 9; i++)
					expectedLogs.add("log_" + _srcId + "_" + i);
				assertEquals("Remaining Log tables check 1", expectedLogs,
						gotLogFromLogInfo);
				assertEquals("Remaining Log tables check 2", expectedLogs,
						gotLogTables);

				List<String> expTabRows = new ArrayList<String>();
				for (int i = 0; i < 405; i++) {
					expTabRows.add(allKeyScns.get(i));
				}
				List<String> gotTabRows = getScnKeyFromTable("tab_" + _srcId);
				assertEquals("Tab Rows Check", expTabRows, gotTabRows);

				List<String> expLogRows = new ArrayList<String>();
				for (int i = 399; i < allKeyScns.size(); i++) {
					expLogRows.add(allKeyScns.get(i));
				}

				List<String> gotLogRows = new ArrayList<String>();
				for (String l : gotLogFromLogInfo) {
					gotLogRows.addAll(getScnKeyFromTable(l));
				}

				assertEquals("Log Rows Check", expLogRows, gotLogRows);
				long gotStartScn = _dao.getBootstrapConn()
						.executeQueryAndGetLong(
								"select logstartscn from bootstrap_sources where id = "
										+ _srcId, -1);
				assertEquals("Expected StartSCN", 399L, gotStartScn);
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				if ((null != applier) && (applier.isAlive()))
					applier.shutdown();

				if (null != cleaner)
					cleaner.close();

				if (null != _dao)
					_dao.close();
			}
		}
	}

	@Test
	public synchronized void testTableBasedCleaneronCatchupWithApplierDB1() {
		System.out.println("testTableBasedCleaneronCatchupWithApplierDB1 !!");

		// Case when applier is at -1
		{
			// Create 10 log tables
			List<Long> seqs = new ArrayList<Long>();

			for (long i = 0; i < 900; i++)
				seqs.add(i);

			List<String> allKeyScns = null;

			try {
				allKeyScns = populateBootstrapDB(100, -1, seqs, null);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			BootstrapCleanerConfig cleanerConfig = new BootstrapCleanerConfig();
			cleanerConfig
					.setBootstrapTypeDefault(BootstrapDBType.BOOTSTRAP_CATCHUP_APPLIER_RUNNING
							.toString());
			cleanerConfig.setEnableOptimizeTableDefault(true);
			cleanerConfig.setEnable(true);
			cleanerConfig.getRetentionDefault().setRetentionQuantity(5);
			cleanerConfig.getRetentionDefault().setRetentionType(
					RetentionType.RETENTION_LOGS.toString());
			List<String> sources = new ArrayList<String>();
			sources.add(_srcName);
			TestApplier applier = new TestApplier("testApplier");
			BootstrapDBCleaner cleaner = null;
			try {
				applier.start();
				cleaner = new BootstrapDBCleaner("testCleaner",
						cleanerConfig.build(), _config.build(), applier,
						sources);
				cleaner.doClean();
				List<String> gotLogFromLogInfo = getRemainingLogsFromLogInfo(_srcId);
				List<String> gotLogTables = getRemainingLogTables(_srcId);
				List<String> expectedLogs = new ArrayList<String>();
				for (int i = 0; i <= 9; i++)
					expectedLogs.add("log_" + _srcId + "_" + i);

				assertEquals("Remaining Log tables check 1", expectedLogs,
						gotLogFromLogInfo);
				assertEquals("Remaining Log tables check 2", expectedLogs,
						gotLogTables);

				List<String> gotTabRows = getScnKeyFromTable("tab_" + _srcId);
				assertEquals("Tab should be empty", 0, gotTabRows.size());

				List<String> expLogRows = new ArrayList<String>();
				for (int i = 0; i < allKeyScns.size(); i++) {
					expLogRows.add(allKeyScns.get(i));
				}

				List<String> gotLogRows = new ArrayList<String>();
				for (String l : gotLogFromLogInfo) {
					gotLogRows.addAll(getScnKeyFromTable(l));
				}

				assertEquals("Log Rows Check", expLogRows, gotLogRows);
				long gotStartScn = _dao.getBootstrapConn()
						.executeQueryAndGetLong(
								"select logstartscn from bootstrap_sources where id = "
										+ _srcId, -1);
				assertEquals("Expected StartSCN", 0L, gotStartScn);
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				if ((null != applier) && (applier.isAlive()))
					applier.shutdown();

				if (null != cleaner)
					cleaner.close();

				if (null != _dao)
					_dao.close();
			}
		}
	}

	@Test
	public synchronized void testTableBasedCleaneronCatchupWithApplierDB2() {
		System.out.println("testTableBasedCleaneronCatchupWithApplierDB2 !!");

		// Case when applier is NOT at -1 and Optimize is disabled
		{
			// Create 10 log tables
			List<Long> seqs = new ArrayList<Long>();

			for (long i = 0; i < 900; i++)
				seqs.add(i);

			List<String> allKeyScns = null;

			try {
				allKeyScns = populateBootstrapDB(100, 605, seqs, null);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			BootstrapCleanerConfig cleanerConfig = new BootstrapCleanerConfig();
			cleanerConfig
					.setBootstrapTypeDefault(BootstrapDBType.BOOTSTRAP_CATCHUP_APPLIER_RUNNING
							.toString());
			cleanerConfig.setEnable(true);
			cleanerConfig.getRetentionDefault().setRetentionQuantity(5);
			cleanerConfig.getRetentionDefault().setRetentionType(
					RetentionType.RETENTION_LOGS.toString());
			List<String> sources = new ArrayList<String>();
			sources.add(_srcName);
			TestApplier applier = new TestApplier("testApplier");
			BootstrapDBCleaner cleaner = null;

			try {
				applier.start();
				cleaner = new BootstrapDBCleaner("testCleaner",
						cleanerConfig.build(), _config.build(), applier,
						sources);
				cleaner.doClean();
				List<String> gotLogFromLogInfo = getRemainingLogsFromLogInfo(_srcId);
				List<String> gotLogTables = getRemainingLogTables(_srcId);
				List<String> expectedLogs = new ArrayList<String>();
				for (int i = 1; i <= 9; i++)
					expectedLogs.add("log_" + _srcId + "_" + i);
				assertEquals("Remaining Log tables check 1", expectedLogs,
						gotLogFromLogInfo);
				assertEquals("Remaining Log tables check 2", expectedLogs,
						gotLogTables);

				List<String> expTabRows = new ArrayList<String>();
				for (int i = 99; i < 605; i++) {
					expTabRows.add(allKeyScns.get(i));
				}
				List<String> gotTabRows = getScnKeyFromTable("tab_" + _srcId);
				assertEquals("Tab Rows Check", expTabRows, gotTabRows);

				List<String> expLogRows = new ArrayList<String>();
				for (int i = 99; i < allKeyScns.size(); i++) {
					expLogRows.add(allKeyScns.get(i));
				}

				List<String> gotLogRows = new ArrayList<String>();
				for (String l : gotLogFromLogInfo) {
					gotLogRows.addAll(getScnKeyFromTable(l));
				}

				assertEquals("Log Rows Check", expLogRows, gotLogRows);
				long gotStartScn = _dao.getBootstrapConn()
						.executeQueryAndGetLong(
								"select logstartscn from bootstrap_sources where id = "
										+ _srcId, -1);
				assertEquals("Expected StartSCN", 99L, gotStartScn);
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				if ((null != applier) && (applier.isAlive()))
					applier.shutdown();

				if (null != cleaner)
					cleaner.close();

				if (null != _dao)
					_dao.close();
			}
		}
	}

	@Test
	public synchronized void testTableBasedCleaneronCatchupWithApplierDB3() {
		System.out.println("testTableBasedCleaneronCatchupWithApplierDB3 !!");

		// Case when applier is NOT at -1 and Optimize is enabled
		{
			// Create 10 log tables
			List<Long> seqs = new ArrayList<Long>();

			for (long i = 0; i < 900; i++)
				seqs.add(i);

			List<String> allKeyScns = null;

			try {
				allKeyScns = populateBootstrapDB(100, 605, seqs, null);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			BootstrapCleanerConfig cleanerConfig = new BootstrapCleanerConfig();
			cleanerConfig
					.setBootstrapTypeDefault(BootstrapDBType.BOOTSTRAP_CATCHUP_APPLIER_RUNNING
							.toString());
			cleanerConfig.setEnable(true);
			cleanerConfig.getRetentionDefault().setRetentionQuantity(5);
			cleanerConfig.getRetentionDefault().setRetentionType(
					RetentionType.RETENTION_LOGS.toString());
			cleanerConfig.setEnableOptimizeTableDefault(true);
			List<String> sources = new ArrayList<String>();
			sources.add(_srcName);
			TestApplier applier = new TestApplier("testApplier");
			BootstrapDBCleaner cleaner = null;

			try {
				applier.start();
				cleaner = new BootstrapDBCleaner("testCleaner",
						cleanerConfig.build(), _config.build(), applier,
						sources);
				cleaner.doClean();
				List<String> gotLogFromLogInfo = getRemainingLogsFromLogInfo(_srcId);
				List<String> gotLogTables = getRemainingLogTables(_srcId);
				List<String> expectedLogs = new ArrayList<String>();
				for (int i = 1; i <= 9; i++)
					expectedLogs.add("log_" + _srcId + "_" + i);
				assertEquals("Remaining Log tables check 1", expectedLogs,
						gotLogFromLogInfo);
				assertEquals("Remaining Log tables check 2", expectedLogs,
						gotLogTables);

				List<String> expTabRows = new ArrayList<String>();
				for (int i = 99; i < 605; i++) {
					expTabRows.add(allKeyScns.get(i));
				}
				List<String> gotTabRows = getScnKeyFromTable("tab_" + _srcId);
				assertEquals("Tab Rows Check", expTabRows, gotTabRows);

				List<String> expLogRows = new ArrayList<String>();
				for (int i = 99; i < allKeyScns.size(); i++) {
					expLogRows.add(allKeyScns.get(i));
				}

				List<String> gotLogRows = new ArrayList<String>();
				for (String l : gotLogFromLogInfo) {
					gotLogRows.addAll(getScnKeyFromTable(l));
				}

				assertEquals("Log Rows Check", expLogRows, gotLogRows);
				long gotStartScn = _dao.getBootstrapConn()
						.executeQueryAndGetLong(
								"select logstartscn from bootstrap_sources where id = "
										+ _srcId, -1);
				assertEquals("Expected StartSCN", 99L, gotStartScn);
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				if ((null != applier) && (applier.isAlive()))
					applier.shutdown();

				if (null != cleaner)
					cleaner.close();

				if (null != _dao)
					_dao.close();
			}
		}
	}

	@Test
	public synchronized void testTimeBasedCleaneronCatchupWithApplierDB1() {
		System.out.println("testTimeBasedCleaneronCatchupWithApplierDB1 !!");

		// Case when applier is at -1
		{
			// Create 10 log tables. Each event is 1 hr apart
			List<Long> seqs = new ArrayList<Long>();
			long currTime = System.currentTimeMillis() * 1000000L;
			final long stepTime = 60 * 60 * 1000L * 1000000L; // 1 hr
			List<Long> timestamps = new ArrayList<Long>();
			for (long i = 0; i < 900; i++) {
				seqs.add(i);
				long t = currTime - ((900 - i) * stepTime);
				timestamps.add(t);
			}

			List<String> allKeyScns = null;

			try {
				allKeyScns = populateBootstrapDB(100, -1, seqs, timestamps);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			BootstrapCleanerConfig cleanerConfig = new BootstrapCleanerConfig();
			cleanerConfig
					.setBootstrapTypeDefault(BootstrapDBType.BOOTSTRAP_CATCHUP_APPLIER_RUNNING
							.toString());
			cleanerConfig.setEnableOptimizeTableDefault(true);
			cleanerConfig.setEnable(true);
			cleanerConfig.getRetentionDefault().setRetentionQuantity(
					(currTime - timestamps.get(400)) / 1000000000L);
			cleanerConfig.getRetentionDefault().setRetentionType(
					RetentionType.RETENTION_SECONDS.toString());
			List<String> sources = new ArrayList<String>();
			sources.add(_srcName);
			TestApplier applier = new TestApplier("testApplier");
			BootstrapDBCleaner cleaner = null;
			try {
				applier.start();
				cleaner = new BootstrapDBCleaner("testCleaner",
						cleanerConfig.build(), _config.build(), applier,
						sources);
				cleaner.doClean();
				List<String> gotLogFromLogInfo = getRemainingLogsFromLogInfo(_srcId);
				List<String> gotLogTables = getRemainingLogTables(_srcId);
				List<String> expectedLogs = new ArrayList<String>();
				for (int i = 0; i <= 9; i++)
					expectedLogs.add("log_" + _srcId + "_" + i);

				assertEquals("Remaining Log tables check 1", expectedLogs,
						gotLogFromLogInfo);
				assertEquals("Remaining Log tables check 2", expectedLogs,
						gotLogTables);

				List<String> gotTabRows = getScnKeyFromTable("tab_" + _srcId);
				assertEquals("Tab should be empty", 0, gotTabRows.size());

				List<String> expLogRows = new ArrayList<String>();
				for (int i = 0; i < allKeyScns.size(); i++) {
					expLogRows.add(allKeyScns.get(i));
				}

				List<String> gotLogRows = new ArrayList<String>();
				for (String l : gotLogFromLogInfo) {
					gotLogRows.addAll(getScnKeyFromTable(l));
				}

				assertEquals("Log Rows Check", expLogRows, gotLogRows);
				long gotStartScn = _dao.getBootstrapConn()
						.executeQueryAndGetLong(
								"select logstartscn from bootstrap_sources where id = "
										+ _srcId, -1);
				assertEquals("Expected StartSCN", 0L, gotStartScn);
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				if ((null != applier) && (applier.isAlive()))
					applier.shutdown();

				if (null != cleaner)
					cleaner.close();

				if (null != _dao)
					_dao.close();
			}
		}
	}

	@Test
	public synchronized void testTimeBasedCleaneronCatchupWithApplierDB2() {
		System.out.println("testTimeBasedCleaneronCatchupWithApplierDB2 !!");

		// Case when applier is above the threshold time limit
		{
			// Create 10 log tables. Each event is 1 hr apart
			List<Long> seqs = new ArrayList<Long>();
			long currTime = System.currentTimeMillis() * 1000000L;
			final long stepTime = 60 * 60 * 1000L * 1000000L; // 1 hr
			List<Long> timestamps = new ArrayList<Long>();
			for (long i = 0; i < 900; i++) {
				seqs.add(i);
				long t = currTime - ((900 - i) * stepTime);
				timestamps.add(t);
			}

			List<String> allKeyScns = null;

			try {
				allKeyScns = populateBootstrapDB(100, 605, seqs, timestamps);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			BootstrapCleanerConfig cleanerConfig = new BootstrapCleanerConfig();
			cleanerConfig
					.setBootstrapTypeDefault(BootstrapDBType.BOOTSTRAP_CATCHUP_APPLIER_RUNNING
							.toString());
			cleanerConfig.setEnable(true);
			cleanerConfig.getRetentionDefault().setRetentionQuantity(
					(currTime - timestamps.get(400)) / 1000000000L);
			cleanerConfig.getRetentionDefault().setRetentionType(
					RetentionType.RETENTION_SECONDS.toString());
			List<String> sources = new ArrayList<String>();
			sources.add(_srcName);
			TestApplier applier = new TestApplier("testApplier");
			BootstrapDBCleaner cleaner = null;

			try {
				applier.start();
				cleaner = new BootstrapDBCleaner("testCleaner",
						cleanerConfig.build(), _config.build(), applier,
						sources);
				cleaner.doClean();
				List<String> gotLogFromLogInfo = getRemainingLogsFromLogInfo(_srcId);
				List<String> gotLogTables = getRemainingLogTables(_srcId);
				List<String> expectedLogs = new ArrayList<String>();
				for (int i = 4; i <= 9; i++)
					expectedLogs.add("log_" + _srcId + "_" + i);
				assertEquals("Remaining Log tables check 1", expectedLogs,
						gotLogFromLogInfo);
				assertEquals("Remaining Log tables check 2", expectedLogs,
						gotLogTables);

				List<String> expTabRows = new ArrayList<String>();
				for (int i = 399; i < 605; i++) {
					expTabRows.add(allKeyScns.get(i));
				}
				List<String> gotTabRows = getScnKeyFromTable("tab_" + _srcId);
				assertEquals("Tab Rows Check", expTabRows, gotTabRows);

				List<String> expLogRows = new ArrayList<String>();
				for (int i = 399; i < allKeyScns.size(); i++) {
					expLogRows.add(allKeyScns.get(i));
				}

				List<String> gotLogRows = new ArrayList<String>();
				for (String l : gotLogFromLogInfo) {
					gotLogRows.addAll(getScnKeyFromTable(l));
				}

				assertEquals("Log Rows Check", expLogRows, gotLogRows);
				long gotStartScn = _dao.getBootstrapConn()
						.executeQueryAndGetLong(
								"select logstartscn from bootstrap_sources where id = "
										+ _srcId, -1);
				assertEquals("Expected StartSCN", 399L, gotStartScn);
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				if ((null != applier) && (applier.isAlive()))
					applier.shutdown();

				if (null != cleaner)
					cleaner.close();

				if (null != _dao)
					_dao.close();
			}
		}
	}

	@Test
	public synchronized void testTimeBasedCleaneronCatchupWithApplierDB3() {
		System.out.println("testTimeBasedCleaneronCatchupWithApplierDB3 !!");

		// Case when applier is below the threshold time limit
		{
			// Create 10 log tables. Each event is 1 hr apart
			List<Long> seqs = new ArrayList<Long>();
			long currTime = System.currentTimeMillis() * 1000000L;
			final long stepTime = 60 * 60 * 1000L * 1000000L; // 1 hr
			List<Long> timestamps = new ArrayList<Long>();
			for (long i = 0; i < 900; i++) {
				seqs.add(i);
				long t = currTime - ((900 - i) * stepTime);
				timestamps.add(t);
			}

			List<String> allKeyScns = null;

			try {
				allKeyScns = populateBootstrapDB(100, 605, seqs, timestamps);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			BootstrapCleanerConfig cleanerConfig = new BootstrapCleanerConfig();
			cleanerConfig
					.setBootstrapTypeDefault(BootstrapDBType.BOOTSTRAP_CATCHUP_APPLIER_RUNNING
							.toString());
			cleanerConfig.setEnable(true);
			cleanerConfig.getRetentionDefault().setRetentionQuantity(
					(currTime - timestamps.get(800)) / 1000000000L);
			cleanerConfig.getRetentionDefault().setRetentionType(
					RetentionType.RETENTION_SECONDS.toString());
			List<String> sources = new ArrayList<String>();
			sources.add(_srcName);
			TestApplier applier = new TestApplier("testApplier");
			BootstrapDBCleaner cleaner = null;

			try {
				applier.start();
				cleaner = new BootstrapDBCleaner("testCleaner",
						cleanerConfig.build(), _config.build(), applier,
						sources);
				cleaner.doClean();
				List<String> gotLogFromLogInfo = getRemainingLogsFromLogInfo(_srcId);
				List<String> gotLogTables = getRemainingLogTables(_srcId);
				List<String> expectedLogs = new ArrayList<String>();
				for (int i = 6; i <= 9; i++)
					expectedLogs.add("log_" + _srcId + "_" + i);
				assertEquals("Remaining Log tables check 1", expectedLogs,
						gotLogFromLogInfo);
				assertEquals("Remaining Log tables check 2", expectedLogs,
						gotLogTables);

				List<String> expTabRows = new ArrayList<String>();
				for (int i = 599; i < 605; i++) {
					expTabRows.add(allKeyScns.get(i));
				}
				List<String> gotTabRows = getScnKeyFromTable("tab_" + _srcId);
				assertEquals("Tab Rows Check", expTabRows, gotTabRows);

				List<String> expLogRows = new ArrayList<String>();
				for (int i = 599; i < allKeyScns.size(); i++) {
					expLogRows.add(allKeyScns.get(i));
				}

				List<String> gotLogRows = new ArrayList<String>();
				for (String l : gotLogFromLogInfo) {
					gotLogRows.addAll(getScnKeyFromTable(l));
				}

				assertEquals("Log Rows Check", expLogRows, gotLogRows);
				long gotStartScn = _dao.getBootstrapConn()
						.executeQueryAndGetLong(
								"select logstartscn from bootstrap_sources where id = "
										+ _srcId, -1);
				assertEquals("Expected StartSCN", 599L, gotStartScn);
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				if ((null != applier) && (applier.isAlive()))
					applier.shutdown();

				if (null != cleaner)
					cleaner.close();

				if (null != _dao)
					_dao.close();
			}
		}
	}

	@Test
	public synchronized void testTimeBasedCleaneronCatchupWithApplierDB5() {
		System.out.println("testTimeBasedCleaneronCatchupWithApplierDB4 !!");

		// Case when applier instance is not provided but forceTabTableCleanup
		// set
		{
			// Create 10 log tables. Each event is 1 hr apart
			List<Long> seqs = new ArrayList<Long>();
			long currTime = System.currentTimeMillis() * 1000000L;
			final long stepTime = 60 * 60 * 1000L * 1000000L; // 1 hr
			List<Long> timestamps = new ArrayList<Long>();
			for (long i = 0; i < 900; i++) {
				seqs.add(i);
				long t = currTime - ((900 - i) * stepTime);
				timestamps.add(t);
			}

			List<String> allKeyScns = null;

			try {
				allKeyScns = populateBootstrapDB(100, 605, seqs, timestamps);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			BootstrapCleanerConfig cleanerConfig = new BootstrapCleanerConfig();
			cleanerConfig
					.setBootstrapTypeDefault(BootstrapDBType.BOOTSTRAP_CATCHUP_APPLIER_RUNNING
							.toString());
			cleanerConfig.setEnable(true);
			cleanerConfig.setEnableForceTabTableCleanupDefault(true);
			cleanerConfig.getRetentionDefault().setRetentionQuantity(
					(currTime - timestamps.get(400)) / 1000000000L);
			cleanerConfig.getRetentionDefault().setRetentionType(
					RetentionType.RETENTION_SECONDS.toString());
			List<String> sources = new ArrayList<String>();
			sources.add(_srcName);
			BootstrapDBCleaner cleaner = null;

			try {
				cleaner = new BootstrapDBCleaner("testCleaner",
						cleanerConfig.build(), _config.build(), null, sources);
				cleaner.doClean();
				List<String> gotLogFromLogInfo = getRemainingLogsFromLogInfo(_srcId);
				List<String> gotLogTables = getRemainingLogTables(_srcId);
				List<String> expectedLogs = new ArrayList<String>();
				for (int i = 4; i <= 9; i++)
					expectedLogs.add("log_" + _srcId + "_" + i);
				assertEquals("Remaining Log tables check 1", expectedLogs,
						gotLogFromLogInfo);
				assertEquals("Remaining Log tables check 2", expectedLogs,
						gotLogTables);

				List<String> expTabRows = new ArrayList<String>();
				for (int i = 399; i < 605; i++) {
					expTabRows.add(allKeyScns.get(i));
				}
				List<String> gotTabRows = getScnKeyFromTable("tab_" + _srcId);
				assertEquals("Tab Rows Check", expTabRows, gotTabRows);

				List<String> expLogRows = new ArrayList<String>();
				for (int i = 399; i < allKeyScns.size(); i++) {
					expLogRows.add(allKeyScns.get(i));
				}

				List<String> gotLogRows = new ArrayList<String>();
				for (String l : gotLogFromLogInfo) {
					gotLogRows.addAll(getScnKeyFromTable(l));
				}

				assertEquals("Log Rows Check", expLogRows, gotLogRows);
				long gotStartScn = _dao.getBootstrapConn()
						.executeQueryAndGetLong(
								"select logstartscn from bootstrap_sources where id = "
										+ _srcId, -1);
				assertEquals("Expected StartSCN", 399L, gotStartScn);
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {

				if (null != cleaner)
					cleaner.close();

				if (null != _dao)
					_dao.close();
			}
		}
	}

	@Test
	public synchronized void testTimeBasedCleaneronCatchupWithApplierDB6() {
		System.out.println("testTimeBasedCleaneronCatchupWithApplierDB4 !!");

		// Case when applier instance is not provided ant forceTabTableCleanup
		// not set
		{
			// Create 10 log tables. Each event is 1 hr apart
			List<Long> seqs = new ArrayList<Long>();
			long currTime = System.currentTimeMillis() * 1000000L;
			final long stepTime = 60 * 60 * 1000L * 1000000L; // 1 hr
			List<Long> timestamps = new ArrayList<Long>();
			for (long i = 0; i < 900; i++) {
				seqs.add(i);
				long t = currTime - ((900 - i) * stepTime);
				timestamps.add(t);
			}

			List<String> allKeyScns = null;

			try {
				allKeyScns = populateBootstrapDB(100, 605, seqs, timestamps);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			BootstrapCleanerConfig cleanerConfig = new BootstrapCleanerConfig();
			cleanerConfig
					.setBootstrapTypeDefault(BootstrapDBType.BOOTSTRAP_CATCHUP_APPLIER_RUNNING
							.toString());
			cleanerConfig.setEnable(true);
			cleanerConfig.setEnableForceTabTableCleanupDefault(false);
			cleanerConfig.getRetentionDefault().setRetentionQuantity(
					(currTime - timestamps.get(400)) / 1000000000L);
			cleanerConfig.getRetentionDefault().setRetentionType(
					RetentionType.RETENTION_SECONDS.toString());
			List<String> sources = new ArrayList<String>();
			sources.add(_srcName);
			BootstrapDBCleaner cleaner = null;

			try {
				cleaner = new BootstrapDBCleaner("testCleaner",
						cleanerConfig.build(), _config.build(), null, sources);
				cleaner.doClean();
				List<String> gotLogFromLogInfo = getRemainingLogsFromLogInfo(_srcId);
				List<String> gotLogTables = getRemainingLogTables(_srcId);
				List<String> expectedLogs = new ArrayList<String>();
				for (int i = 4; i <= 9; i++)
					expectedLogs.add("log_" + _srcId + "_" + i);
				assertEquals("Remaining Log tables check 1", expectedLogs,
						gotLogFromLogInfo);
				assertEquals("Remaining Log tables check 2", expectedLogs,
						gotLogTables);

				List<String> expTabRows = new ArrayList<String>();
				for (int i = 0; i < 605; i++) {
					expTabRows.add(allKeyScns.get(i));
				}
				List<String> gotTabRows = getScnKeyFromTable("tab_" + _srcId);
				assertEquals("Tab Rows Check", expTabRows, gotTabRows);

				List<String> expLogRows = new ArrayList<String>();
				for (int i = 399; i < allKeyScns.size(); i++) {
					expLogRows.add(allKeyScns.get(i));
				}

				List<String> gotLogRows = new ArrayList<String>();
				for (String l : gotLogFromLogInfo) {
					gotLogRows.addAll(getScnKeyFromTable(l));
				}

				assertEquals("Log Rows Check", expLogRows, gotLogRows);
				long gotStartScn = _dao.getBootstrapConn()
						.executeQueryAndGetLong(
								"select logstartscn from bootstrap_sources where id = "
										+ _srcId, -1);
				assertEquals("Expected StartSCN", 399L, gotStartScn);
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {

				if (null != cleaner)
					cleaner.close();

				if (null != _dao)
					_dao.close();
			}
		}
	}

	@Test
	public synchronized void testTimeBasedCleaneronCatchupWithApplierDB4() {
		System.out.println("testTimeBasedCleaneronCatchupWithApplierDB4 !!");

		// Case when applier is above the threshold time limit and optimize
		// enabled
		{
			// Create 10 log tables. Each event is 1 hr apart
			List<Long> seqs = new ArrayList<Long>();
			long currTime = System.currentTimeMillis() * 1000000L;
			final long stepTime = 60 * 60 * 1000L * 1000000L; // 1 hr
			List<Long> timestamps = new ArrayList<Long>();
			for (long i = 0; i < 900; i++) {
				seqs.add(i);
				long t = currTime - ((900 - i) * stepTime);
				timestamps.add(t);
			}

			List<String> allKeyScns = null;

			try {
				allKeyScns = populateBootstrapDB(100, 605, seqs, timestamps);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			BootstrapCleanerConfig cleanerConfig = new BootstrapCleanerConfig();
			cleanerConfig
					.setBootstrapTypeDefault(BootstrapDBType.BOOTSTRAP_CATCHUP_APPLIER_RUNNING
							.toString());
			cleanerConfig.setEnable(true);
			cleanerConfig.setEnableOptimizeTableDefault(true);
			cleanerConfig.getRetentionDefault().setRetentionQuantity(
					(currTime - timestamps.get(400)) / 1000000000L);
			cleanerConfig.getRetentionDefault().setRetentionType(
					RetentionType.RETENTION_SECONDS.toString());
			List<String> sources = new ArrayList<String>();
			sources.add(_srcName);
			TestApplier applier = new TestApplier("testApplier");
			BootstrapDBCleaner cleaner = null;

			try {
				applier.start();
				cleaner = new BootstrapDBCleaner("testCleaner",
						cleanerConfig.build(), _config.build(), applier,
						sources);
				cleaner.doClean();
				List<String> gotLogFromLogInfo = getRemainingLogsFromLogInfo(_srcId);
				List<String> gotLogTables = getRemainingLogTables(_srcId);
				List<String> expectedLogs = new ArrayList<String>();
				for (int i = 4; i <= 9; i++)
					expectedLogs.add("log_" + _srcId + "_" + i);
				assertEquals("Remaining Log tables check 1", expectedLogs,
						gotLogFromLogInfo);
				assertEquals("Remaining Log tables check 2", expectedLogs,
						gotLogTables);

				List<String> expTabRows = new ArrayList<String>();
				for (int i = 399; i < 605; i++) {
					expTabRows.add(allKeyScns.get(i));
				}
				List<String> gotTabRows = getScnKeyFromTable("tab_" + _srcId);
				assertEquals("Tab Rows Check", expTabRows, gotTabRows);

				List<String> expLogRows = new ArrayList<String>();
				for (int i = 399; i < allKeyScns.size(); i++) {
					expLogRows.add(allKeyScns.get(i));
				}

				List<String> gotLogRows = new ArrayList<String>();
				for (String l : gotLogFromLogInfo) {
					gotLogRows.addAll(getScnKeyFromTable(l));
				}

				assertEquals("Log Rows Check", expLogRows, gotLogRows);
				long gotStartScn = _dao.getBootstrapConn()
						.executeQueryAndGetLong(
								"select logstartscn from bootstrap_sources where id = "
										+ _srcId, -1);
				assertEquals("Expected StartSCN", 399L, gotStartScn);
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				if ((null != applier) && (applier.isAlive()))
					applier.shutdown();

				if (null != cleaner)
					cleaner.close();

				if (null != _dao)
					_dao.close();
			}
		}
	}

	@Test
	public synchronized void testTimeBasedCleaneronCatchupOnlyDB1() {
		System.out.println("testTimeBasedCleaneronCatchupOnlyDB1 !!");

		// Case when applier is at -1
		{
			// Create 10 log tables. Each event is 1 hr apart
			List<Long> seqs = new ArrayList<Long>();
			long currTime = System.currentTimeMillis() * 1000000L;
			final long stepTime = 60 * 60 * 1000L * 1000000L; // 1 hr
			List<Long> timestamps = new ArrayList<Long>();
			for (long i = 0; i < 900; i++) {
				seqs.add(i);
				long t = currTime - ((900 - i) * stepTime);
				timestamps.add(t);
			}

			List<String> allKeyScns = null;

			try {
				allKeyScns = populateBootstrapDB(100, -1, seqs, timestamps);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			BootstrapCleanerConfig cleanerConfig = new BootstrapCleanerConfig();
			cleanerConfig
					.setBootstrapTypeDefault(BootstrapDBType.BOOTSTRAP_CATCHUP_APPLIER_NOT_RUNNING
							.toString());
			cleanerConfig.setEnable(true);
			cleanerConfig.getRetentionDefault().setRetentionQuantity(
					(currTime - timestamps.get(400)) / 1000000000L);
			cleanerConfig.getRetentionDefault().setRetentionType(
					RetentionType.RETENTION_SECONDS.toString());
			List<String> sources = new ArrayList<String>();
			sources.add(_srcName);
			TestApplier applier = new TestApplier("testApplier");
			BootstrapDBCleaner cleaner = null;

			try {
				cleaner = new BootstrapDBCleaner("testCleaner",
						cleanerConfig.build(), _config.build(), applier,
						sources);
				cleaner.doClean();
				List<String> gotLogFromLogInfo = getRemainingLogsFromLogInfo(_srcId);
				List<String> gotLogTables = getRemainingLogTables(_srcId);
				List<String> expectedLogs = new ArrayList<String>();
				for (int i = 4; i <= 9; i++)
					expectedLogs.add("log_" + _srcId + "_" + i);

				assertEquals("Remaining Log tables check 1", expectedLogs,
						gotLogFromLogInfo);
				assertEquals("Remaining Log tables check 2", expectedLogs,
						gotLogTables);

				List<String> gotTabRows = getScnKeyFromTable("tab_" + _srcId);
				assertEquals("Tab should be empty", 0, gotTabRows.size());

				List<String> expLogRows = new ArrayList<String>();
				for (int i = 399; i < allKeyScns.size(); i++) {
					expLogRows.add(allKeyScns.get(i));
				}

				List<String> gotLogRows = new ArrayList<String>();
				for (String l : gotLogFromLogInfo) {
					gotLogRows.addAll(getScnKeyFromTable(l));
				}

				assertEquals("Log Rows Check", expLogRows, gotLogRows);
				long gotStartScn = _dao.getBootstrapConn()
						.executeQueryAndGetLong(
								"select logstartscn from bootstrap_sources where id = "
										+ _srcId, -1);
				assertEquals("Expected StartSCN", 399L, gotStartScn);
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				if (null != cleaner)
					cleaner.close();

				if (null != _dao)
					_dao.close();
			}
		}
	}

	@Test
	public synchronized void testTimeBasedCleaneronCatchupOnlyDB2() {
		System.out.println("testTimeBasedCleaneronCatchupOnlyDB2 !!");

		// Case when applier is NOT at -1
		{
			// Create 10 log tables. Each event is 1 hr apart
			List<Long> seqs = new ArrayList<Long>();
			long currTime = System.currentTimeMillis() * 1000000L;
			final long stepTime = 60 * 60 * 1000L * 1000000L; // 1 hr
			List<Long> timestamps = new ArrayList<Long>();
			for (long i = 0; i < 900; i++) {
				seqs.add(i);
				long t = currTime - ((900 - i) * stepTime);
				timestamps.add(t);
			}

			List<String> allKeyScns = null;

			try {
				allKeyScns = populateBootstrapDB(100, 205, seqs, timestamps);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			BootstrapCleanerConfig cleanerConfig = new BootstrapCleanerConfig();
			cleanerConfig
					.setBootstrapTypeDefault(BootstrapDBType.BOOTSTRAP_CATCHUP_APPLIER_NOT_RUNNING
							.toString());
			cleanerConfig.setEnable(true);
			cleanerConfig.getRetentionDefault().setRetentionQuantity(
					(currTime - timestamps.get(400)) / 1000000000L);
			cleanerConfig.getRetentionDefault().setRetentionType(
					RetentionType.RETENTION_SECONDS.toString());
			List<String> sources = new ArrayList<String>();
			sources.add(_srcName);
			TestApplier applier = new TestApplier("testApplier");
			BootstrapDBCleaner cleaner = null;

			try {
				cleaner = new BootstrapDBCleaner("testCleaner",
						cleanerConfig.build(), _config.build(), applier,
						sources);
				cleaner.doClean();
				List<String> gotLogFromLogInfo = getRemainingLogsFromLogInfo(_srcId);
				List<String> gotLogTables = getRemainingLogTables(_srcId);
				List<String> expectedLogs = new ArrayList<String>();
				for (int i = 4; i <= 9; i++)
					expectedLogs.add("log_" + _srcId + "_" + i);
				assertEquals("Remaining Log tables check 1", expectedLogs,
						gotLogFromLogInfo);
				assertEquals("Remaining Log tables check 2", expectedLogs,
						gotLogTables);

				List<String> expTabRows = new ArrayList<String>();
				for (int i = 0; i < 205; i++) {
					expTabRows.add(allKeyScns.get(i));
				}
				List<String> gotTabRows = getScnKeyFromTable("tab_" + _srcId);
				assertEquals("Tab Rows Check", expTabRows, gotTabRows);

				List<String> expLogRows = new ArrayList<String>();
				for (int i = 399; i < allKeyScns.size(); i++) {
					expLogRows.add(allKeyScns.get(i));
				}

				List<String> gotLogRows = new ArrayList<String>();
				for (String l : gotLogFromLogInfo) {
					gotLogRows.addAll(getScnKeyFromTable(l));
				}

				assertEquals("Log Rows Check", expLogRows, gotLogRows);
				long gotStartScn = _dao.getBootstrapConn()
						.executeQueryAndGetLong(
								"select logstartscn from bootstrap_sources where id = "
										+ _srcId, -1);
				assertEquals("Expected StartSCN", 399L, gotStartScn);
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				if (null != cleaner)
					cleaner.close();

				if (null != _dao)
					_dao.close();
			}
		}
	}

	@Test
	public synchronized void testTableBasedCleaneronCatchupOnlyDB1() {
		System.out.println("testTableBasedCleaneronCatchupOnlyDB1 !!");

		// Case when applier is at -1
		{
			// Create 10 log tables
			List<Long> seqs = new ArrayList<Long>();

			for (long i = 0; i < 900; i++)
				seqs.add(i);

			List<String> allKeyScns = null;

			try {
				allKeyScns = populateBootstrapDB(100, -1, seqs, null);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			BootstrapCleanerConfig cleanerConfig = new BootstrapCleanerConfig();
			cleanerConfig
					.setBootstrapTypeDefault(BootstrapDBType.BOOTSTRAP_CATCHUP_APPLIER_NOT_RUNNING
							.toString());
			cleanerConfig.setEnable(true);
			cleanerConfig.getRetentionDefault().setRetentionQuantity(5);
			cleanerConfig.getRetentionDefault().setRetentionType(
					RetentionType.RETENTION_LOGS.toString());
			List<String> sources = new ArrayList<String>();
			sources.add(_srcName);
			TestApplier applier = new TestApplier("testApplier");
			BootstrapDBCleaner cleaner = null;

			try {
				cleaner = new BootstrapDBCleaner("testCleaner",
						cleanerConfig.build(), _config.build(), applier,
						sources);
				cleaner.doClean();
				List<String> gotLogFromLogInfo = getRemainingLogsFromLogInfo(_srcId);
				List<String> gotLogTables = getRemainingLogTables(_srcId);
				List<String> expectedLogs = new ArrayList<String>();
				for (int i = 4; i <= 9; i++)
					expectedLogs.add("log_" + _srcId + "_" + i);

				assertEquals("Remaining Log tables check 1", expectedLogs,
						gotLogFromLogInfo);
				assertEquals("Remaining Log tables check 2", expectedLogs,
						gotLogTables);

				List<String> gotTabRows = getScnKeyFromTable("tab_" + _srcId);
				assertEquals("Tab should be empty", 0, gotTabRows.size());

				List<String> expLogRows = new ArrayList<String>();
				for (int i = 399; i < allKeyScns.size(); i++) {
					expLogRows.add(allKeyScns.get(i));
				}

				List<String> gotLogRows = new ArrayList<String>();
				for (String l : gotLogFromLogInfo) {
					gotLogRows.addAll(getScnKeyFromTable(l));
				}

				assertEquals("Log Rows Check", expLogRows, gotLogRows);
				long gotStartScn = _dao.getBootstrapConn()
						.executeQueryAndGetLong(
								"select logstartscn from bootstrap_sources where id = "
										+ _srcId, -1);
				assertEquals("Expected StartSCN", 399L, gotStartScn);
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				if (null != cleaner)
					cleaner.close();

				if (null != _dao)
					_dao.close();
			}
		}
	}

	@Test
	public synchronized void testTableBasedCleaneronCatchupOnlyDB2() {
		System.out.println("testTableBasedCleaneronCatchupOnlyDB2 !!");

		// Case when applier is NOT at -1
		{
			// Create 10 log tables
			List<Long> seqs = new ArrayList<Long>();

			for (long i = 0; i < 900; i++)
				seqs.add(i);

			List<String> allKeyScns = null;

			try {
				allKeyScns = populateBootstrapDB(100, 205, seqs, null);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			BootstrapCleanerConfig cleanerConfig = new BootstrapCleanerConfig();
			cleanerConfig
					.setBootstrapTypeDefault(BootstrapDBType.BOOTSTRAP_CATCHUP_APPLIER_NOT_RUNNING
							.toString());
			cleanerConfig.setEnable(true);
			cleanerConfig.getRetentionDefault().setRetentionQuantity(5);
			cleanerConfig.getRetentionDefault().setRetentionType(
					RetentionType.RETENTION_LOGS.toString());
			List<String> sources = new ArrayList<String>();
			sources.add(_srcName);
			TestApplier applier = new TestApplier("testApplier");
			BootstrapDBCleaner cleaner = null;

			try {
				cleaner = new BootstrapDBCleaner("testCleaner",
						cleanerConfig.build(), _config.build(), applier,
						sources);
				cleaner.doClean();
				List<String> gotLogFromLogInfo = getRemainingLogsFromLogInfo(_srcId);
				List<String> gotLogTables = getRemainingLogTables(_srcId);
				List<String> expectedLogs = new ArrayList<String>();
				for (int i = 4; i <= 9; i++)
					expectedLogs.add("log_" + _srcId + "_" + i);
				assertEquals("Remaining Log tables check 1", expectedLogs,
						gotLogFromLogInfo);
				assertEquals("Remaining Log tables check 2", expectedLogs,
						gotLogTables);

				List<String> expTabRows = new ArrayList<String>();
				for (int i = 0; i < 205; i++) {
					expTabRows.add(allKeyScns.get(i));
				}
				List<String> gotTabRows = getScnKeyFromTable("tab_" + _srcId);
				assertEquals("Tab Rows Check", expTabRows, gotTabRows);

				List<String> expLogRows = new ArrayList<String>();
				for (int i = 399; i < allKeyScns.size(); i++) {
					expLogRows.add(allKeyScns.get(i));
				}

				List<String> gotLogRows = new ArrayList<String>();
				for (String l : gotLogFromLogInfo) {
					gotLogRows.addAll(getScnKeyFromTable(l));
				}

				assertEquals("Log Rows Check", expLogRows, gotLogRows);
				long gotStartScn = _dao.getBootstrapConn()
						.executeQueryAndGetLong(
								"select logstartscn from bootstrap_sources where id = "
										+ _srcId, -1);
				assertEquals("Expected StartSCN", 399L, gotStartScn);
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				if (null != cleaner)
					cleaner.close();

				if (null != _dao)
					_dao.close();
			}
		}
	}

	private void insertLogEvent(long seq, Long key, int logId)
			throws SQLException {
		String sql = getLogStmt(_srcId, logId);
		PreparedStatement stmt = null;
		try {
			Connection conn = _dao.getBootstrapConn().getDBConn();

			stmt = conn.prepareStatement(sql);

			stmt.setLong(1, seq);
			stmt.setLong(2, seq);
			stmt.setString(3, key.toString());
			// Reuse the iStream to set the blob
			_bufStream.reset();
			stmt.setBlob(4, _bufStream, _buf.position());
			stmt.executeUpdate();
			conn.commit();
		} finally {
			DBHelper.close(stmt);
		}
	}

	private void insertTabEvent(long seq, Long key, int logId)
			throws SQLException {
		String sql = getTabStmt(_srcId);
		PreparedStatement stmt = null;
		try {
			Connection conn = _dao.getBootstrapConn().getDBConn();

			stmt = conn.prepareStatement(sql);

			stmt.setLong(1, seq);
			stmt.setString(2, key.toString());
			// Reuse the iStream to set the blob
			_bufStream2.reset();
			stmt.setBlob(3, _bufStream2, _buf.position());
			stmt.setLong(4, seq);
			_bufStream3.reset();
			stmt.setBlob(5, _bufStream3, _buf.position());
			stmt.executeUpdate();
			conn.commit();
		} finally {
			DBHelper.close(stmt);
		}
	}

	private void updateApplierStateTable(int logId, long seq, int rId)
			throws SQLException {
		String sql = getApplierLogPosStmt();
		PreparedStatement stmt = null;
		try {
			Connection conn = _dao.getBootstrapConn().getDBConn();
			stmt = conn.prepareStatement(sql);
			stmt.setInt(1, logId);
			stmt.setInt(2, rId);
			stmt.setLong(3, seq);
			stmt.setInt(4, _srcId);
			stmt.executeUpdate();
			conn.commit();
		} finally {
			DBHelper.close(stmt);
		}
	}

	private void updateProducerStateTable(int logId, boolean useDefaultScn)
			throws SQLException {
		String sql = getProducerLogPosStmt();
		PreparedStatement stmt = null;
		try {
			Connection conn = _dao.getBootstrapConn().getDBConn();
			stmt = conn.prepareStatement(sql);
			stmt.setInt(1, logId);
			if (!useDefaultScn) {
				stmt.setInt(2, getMaxRowIdFromLog(logId));
				stmt.setLong(3, getMaxWindowScnFromLog(logId));
			} else {
				stmt.setInt(2, -1);
				stmt.setInt(3, -1);
			}

			stmt.setInt(4, _srcId);
			stmt.executeUpdate();
			conn.commit();
		} finally {
			DBHelper.close(stmt);
		}
	}

	private void updateLogInfoTable(int logId, boolean useDefaultScn)
			throws SQLException {
		String sql = getLogInfoStmt();
		PreparedStatement stmt = null;
		try {
			long minScn = -1;
			long maxScn = -1;
			int maxRid = -1;

			if (!useDefaultScn) {
				minScn = getMinWindowScnFromLog(logId);
				maxScn = getMaxWindowScnFromLog(logId);
				maxRid = getMaxRowIdFromLog(logId);
			}

			Connection conn = _dao.getBootstrapConn().getDBConn();
			stmt = conn.prepareStatement(sql);
			stmt.setInt(1, _srcId);
			stmt.setInt(2, logId);
			stmt.setLong(3, minScn);
			stmt.setLong(4, maxScn);
			stmt.setInt(5, maxRid);
			stmt.setLong(6, minScn);
			stmt.setLong(7, maxScn);
			stmt.setInt(8, maxRid);
			stmt.executeUpdate();
			conn.commit();
		} finally {
			DBHelper.close(stmt);
		}
	}

	private long getMaxWindowScnFromLog(int logId) throws SQLException {
		String table = "log_" + _srcId + "_" + logId;
		String sql = "select max(windowscn) from " + table;
		long scn = -1;

		Statement stmt = null;
		ResultSet rs = null;
		try {
			Connection conn = _dao.getBootstrapConn().getDBConn();

			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			if (rs.next())
				scn = rs.getInt(1);
		} finally {
			DBHelper.close(rs, stmt, null);
		}
		return scn;
	}

	private long getMinWindowScnFromLog(int logId) throws SQLException {
		String table = "log_" + _srcId + "_" + logId;
		String sql = "select min(windowscn) from " + table;
		long scn = -1;

		Statement stmt = null;
		ResultSet rs = null;
		try {
			Connection conn = _dao.getBootstrapConn().getDBConn();

			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			if (rs.next())
				scn = rs.getInt(1);
		} finally {
			DBHelper.close(rs, stmt, null);
		}
		return scn;
	}

	private int getMaxRowIdFromLog(int logId) throws SQLException {
		String table = "log_" + _srcId + "_" + logId;
		String sql = "select max(id) from " + table;
		int rId = -1;

		Statement stmt = null;
		ResultSet rs = null;
		try {
			Connection conn = _dao.getBootstrapConn().getDBConn();

			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			if (rs.next())
				rId = rs.getInt(1);
		} finally {
			DBHelper.close(rs, stmt, null);
		}
		return rId;
	}

	private String getLogStmt(int srcId, int logId) {
		String table = "log_" + srcId + "_" + logId;
		String sql = "insert into " + table
				+ "(scn,windowscn,srckey,val) values (? , ? , ? , ?) ";
		return sql;
	}

	private String getTabStmt(int srcId) {
		String table = "tab_" + srcId;
		String sql = "insert into "
				+ table
				+ "(scn,srckey,val) values (? , ? , ?) on duplicate key update scn = ?, val = ? ";
		return sql;
	}

	private String getProducerLogPosStmt() {
		StringBuilder sql = new StringBuilder();
		sql.append("update bootstrap_producer_state set logid = ?, rid = ? , windowscn = ? where srcid = ?");
		return sql.toString();
	}

	private String getApplierLogPosStmt() {
		StringBuilder sql = new StringBuilder();
		sql.append("update bootstrap_applier_state set logid = ?, rid = ? , windowscn = ? where srcid = ?");
		return sql.toString();
	}

	private String getLogInfoStmt() {
		StringBuilder sql = new StringBuilder();
		sql.append("insert into bootstrap_loginfo values ( ?, ?, ?, ?, ?, 0 ) on duplicate key update minwindowscn = ?, maxwindowscn = ?, maxrid = ? ");
		return sql.toString();
	}

	private List<String> populateBootstrapDB(int numEventsPerLogTable,
			int applierPos, List<Long> seq, List<Long> currentTimeMillisstamps)
			throws KeyTypeNotImplementedException, UnsupportedKeyException,
			SQLException {
		List<String> scnKeys = new ArrayList<String>();

		String schemaId = "1234567891234567";
		String value = "value";
		int numInserted = 1;
		int logId = 0;
		int i = 0;
		for (Long s : seq) {

			if (numInserted % (numEventsPerLogTable) == 0) {
				updateLogInfoTable(logId, false);
				logId++;
				updateProducerStateTable(logId, true);
				updateLogInfoTable(logId, true);
				_dao.createNewLogTable(_srcId);
			}

			scnKeys.add(s + "_" + s);

			DbusEventKey key = new DbusEventKey(s);
			DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT, s,
					(short) 0, (short) 0,
					(null == currentTimeMillisstamps) ? 100L
							: currentTimeMillisstamps.get(i), (short) _srcId,
					schemaId.getBytes(), value.getBytes(), false, true);
			_buf.clear();
			int size = DbusEvent.serializeEvent(key, _buf, eventInfo);
			LOG.debug("Created DbusEvent of Size :" + size);
			insertLogEvent(s, key.getLongKey(), logId);
			updateProducerStateTable(logId, false);

			if (numInserted <= applierPos) {
				insertTabEvent(s, key.getLongKey(), logId);

				if (numInserted == applierPos)
					updateApplierStateTable(logId, s, getMaxRowIdFromLog(logId));
			}
			numInserted++;
			i++;
		}
		updateLogInfoTable(logId, false);
		return scnKeys;
	}

	private List<String> getScnKeyFromTable(String table) throws SQLException {
		String sql = " select scn, srckey from " + table + " order by id";
		List<String> res = new ArrayList<String>();

		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = _dao.getBootstrapConn().getDBConn().createStatement();
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				long scn = rs.getLong(1);
				String key = rs.getString(2);

				String r = key + "_" + scn;
				res.add(r);
			}
		} finally {
			DBHelper.close(rs, stmt, null);
		}
		return res;
	}

	private List<String> getRemainingLogsFromLogInfo(int srcId)
			throws SQLException {
		List<String> logIds = new ArrayList<String>();

		String sql = "select logId from bootstrap_loginfo where srcid = "
				+ srcId + " and deleted != 1";
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = _dao.getBootstrapConn().getDBConn().createStatement();
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				int logId = rs.getInt(1);

				logIds.add("log_" + srcId + "_" + logId);
			}
		} finally {
			DBHelper.close(rs, stmt, null);
		}
		Collections.sort(logIds);
		return logIds;
	}

	private List<String> getRemainingLogTables(int srcId) throws SQLException {
		List<String> logIds = new ArrayList<String>();
		String pattern = "log_" + srcId + "_%";
		String sql = "show tables like '" + pattern + "'";
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = _dao.getBootstrapConn().getDBConn().createStatement();
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				logIds.add(rs.getString(1));
			}
		} finally {
			DBHelper.close(rs, stmt, null);
		}
		Collections.sort(logIds);
		return logIds;
	}

	public static class TestApplier extends BootstrapProducerThreadBase {
		public boolean isPauseCalled = false;
		public boolean isUnPauseCalled = false;

		public TestApplier(String name) {
			super(name);
		}

		@Override
		public void unpause() throws InterruptedException {
			isUnPauseCalled = true;
			super.unpause();
		}

		@Override
		public void pause() throws InterruptedException {
			isPauseCalled = true;
			super.pause();
		}

		@Override
		public void run() {
			try {
				while (!isShutdownRequested()) {
					if (isPauseRequested()) {
						LOG.info("Pause requested for applier. Pausing !!");
						signalPause();
						LOG.info("Pausing. Waiting for resume command");
						awaitUnPauseRequest();
						LOG.info("Resume requested for applier. Resuming !!");
						signalResumed();
						LOG.info("Applier resumed !!");
					}
					Thread.sleep(100);
				}
			} catch (Exception ex) {
				LOG.error("Got exception in the Dummy Applier thread ", ex);
			}

			doShutdownNotify();
		}
	}
}
