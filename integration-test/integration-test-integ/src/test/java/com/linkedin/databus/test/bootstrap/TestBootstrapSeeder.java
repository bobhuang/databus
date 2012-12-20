package com.linkedin.databus.test.bootstrap;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import oracle.jdbc.pool.OracleDataSource;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.bootstrap.utils.BootstrapDBSeeder;
import com.linkedin.databus.bootstrap.utils.BootstrapSeederMain;
import com.linkedin.databus.client.DbusEventAvroDecoder;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.util.RngUtils;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.VersionedSchemaSet;
import com.linkedin.databus2.util.DBHelper;
import com.linkedin.events.bizfollow.bizfollow.BizFollow;

public class TestBootstrapSeeder {

	private final OracleDataSource _dataSource;
	private final BootstrapDBSeeder _seeder;
	private final SeedTester        _seedTester;
	private final boolean                 _txLogExist = false;

	private static String[] seederArgs = new String[4];
	private static final String bootstrapConfig = "integration-test/config/bootstrap-seeder-config.properties";


	public TestBootstrapSeeder()
	{
		_dataSource = null;
		_seeder = null;
		_seedTester = null;
	}

	@Test
	public void testNothing()
	{}


	public String getDestTableName(SeedTester tester)
	{
		return  "tab_" + tester.getSrcId();
	}

	public String getDestQueryCmd(SeedTester tester)
	{
	    StringBuilder sql = new StringBuilder();
	    sql.append("select * from ");
	    sql.append(getDestTableName(tester));
	    if ( tester.isNumericKey())
	    	sql.append(" order by length(id),id");
	    else
	    	sql.append(" order by id");
	    return sql.toString();
	}

	public String getSrcQueryCmd(SeedTester tester)
	{
	  StringBuilder sql = new StringBuilder();
	  sql.append("select * from ");
	  sql.append(tester.getSrcTableName());
	  sql.append(" order by ");
	  sql.append(tester.getPrimaryKeyName());
	  return sql.toString();
	}

	public String getDestDropCmd(String table)
	{
	  StringBuilder sql = new StringBuilder();
	  sql.append("drop table if exists ");
	  sql.append(table);
	  return sql.toString();
	}

	public void compareTables(SeedTester tester)
	{
		byte[] b = new byte[1024 * 1024];
		ByteBuffer buffer = ByteBuffer.wrap(b);

		PreparedStatement srcStmt = null;
		PreparedStatement destStmt = null;
		ResultSet srcRs = null;
		ResultSet destRs = null;
		try
		{
		  Connection srcConn = _dataSource.getConnection();
		  Connection destConn = _seeder.getConnection();
		  String srcQuery = getSrcQueryCmd(tester);
		  String destQuery = getDestQueryCmd(tester);

		  srcStmt  = srcConn.prepareStatement(srcQuery);
		  destStmt = destConn.prepareStatement(destQuery);

		  srcRs = srcStmt.executeQuery();
		  destRs = destStmt.executeQuery();

		  while ( (srcRs.next()) && (destRs.next()))
		  {
			  buffer.put(destRs.getBytes("val"));
			  DbusEvent destEvent = new DbusEvent(buffer,0);
			  tester.compareRecord(srcRs, destEvent);
			  buffer.clear();
		  }

		  Assert.assertTrue(! srcRs.next());
		  Assert.assertTrue(! destRs.next());

		} catch ( SQLException sqlEx) {
			throw new RuntimeException("Error comparing results ", sqlEx);
		} finally {
			DBHelper.close(srcRs, srcStmt, null);
			DBHelper.close(destRs, destStmt, null);
		}
	}

	public void cleanupOracleTable(Connection conn, SeedTester tester)
	{
		PreparedStatement srcStmt = null;
		ResultSet srcRs = null;

		try
		{
			StringBuilder sql = new StringBuilder();
			String tableName = tester.getSrcTableName();
			sql.append("select count(*) from all_tables where table_name = '");
			sql.append(tableName.toUpperCase()).append("'");

			String countCmd = sql.toString();

			sql.delete(0,sql.length());
			sql.append("drop table ").append(tester.getSrcTableName());
			String dropCmd = sql.toString();

			srcStmt = conn.prepareStatement(countCmd);
			srcRs = srcStmt.executeQuery();

			srcRs.next();
			int count = srcRs.getInt(1);
			if ( count > 0)
			{
		      System.out.println("Table " + tester.getSrcTableName() + " exist and will be deleted!!");

			  DBHelper.close(srcRs, srcStmt, null);
			  srcRs = null;
			  srcStmt = null;
			  srcStmt = conn.prepareStatement(dropCmd);
			  srcStmt.executeUpdate();
			} else {
			  System.out.println("Table " + tester.getSrcTableName() + " doesnt exist !!");
			}
		} catch ( SQLException sqlEx) {
			throw new RuntimeException("Got exception while cleaning up Oracle DB", sqlEx);
		} finally {
			DBHelper.close(srcRs, srcStmt, null);
		}
	}

	public void cleanup(SeedTester tester)
	{
		String destDrop = getDestDropCmd(tester.getSrcTableName());
		PreparedStatement destStmt = null;
		try
		{
		  Connection srcConn = _dataSource.getConnection();
		  cleanupOracleTable(srcConn, tester);

		  Connection destConn = _seeder.getConnection();
		  destStmt = destConn.prepareStatement(destDrop);
		  destStmt.executeUpdate();
		} catch ( SQLException sqlEx) {
			throw new RuntimeException("Unable to cleanup DB", sqlEx);
		} finally {
			DBHelper.close(destStmt);
		}
	}

	public void runTester()
	{
	  try
	  {
		Connection conn = _dataSource.getConnection();

		cleanup(_seedTester);

		createSrcTable(conn,_seedTester);

		populateTable(conn,100, _seedTester);

		seederArgs[3] = _seedTester.getSourcesConfigFile();
		runSeeder(seederArgs);

		compareTables(_seedTester);

		cleanup(_seedTester);
	  } catch (SQLException sqlEx) {
		 throw new RuntimeException("Unable to execute tester", sqlEx);
	  }
	}

	public boolean doesOracleTableExist(Connection conn, String table)
	{
	  StringBuilder sql = new StringBuilder();
	  sql.append("select count(*) from all_tables where table_name = '");
	  sql.append(table.toUpperCase()).append("'");

	  PreparedStatement stmt = null;
	  ResultSet rs = null;
	  boolean exist = false;

	  try
	  {
	    String cmd1 = sql.toString();
	    stmt = conn.prepareStatement(cmd1);
	    rs = stmt.executeQuery();

	    rs.next();
	    if (rs.getInt(1) > 0)
	      exist = true;
	  } catch (SQLException sqlEx) {
		  throw new RuntimeException("Unable to get table existence for table :" + table, sqlEx );
	  } finally {
		  DBHelper.close(rs,stmt,null);
	  }
	  return exist;
	}

	public void createTxLog(Connection conn)
	{
	  String table = "sy$txlog";

	  boolean exist = false; // doesOracleTableExist(conn, table);

	  System.out.println("sy$txlog table exist ?" + exist);

	  if (exist)
		return;

	  StringBuilder sqlB = new StringBuilder();
	  sqlB.append("create table ");
	  sqlB.append(table);
	  sqlB.append("(txn number not null, scn number not null,");
	  sqlB.append(" mask number, ts timestamp(6) not null)");

	  PreparedStatement stmt = null;
	  try
	  {
	    String cmd = sqlB.toString();
	    System.out.println("CreateTxLog command:" + cmd);
	    stmt = conn.prepareStatement(cmd);
	    stmt.executeUpdate();
	  } catch ( SQLException sqlEx) {
		sqlEx.printStackTrace();
		// throw new RuntimeException("Unable to create sy$txlog", sqlEx);
	  } finally {
		DBHelper.close(stmt);
	  }
	}

    public void createSrcTable(Connection conn, SeedTester tester)
    {
      createTxLog(conn);
      String sql = tester.getCreateCmd();

      System.out.println("Command:" + sql);
      PreparedStatement addSrcTabStmt = null;
      try
      {
        addSrcTabStmt = conn.prepareStatement(sql.toString());
        addSrcTabStmt.executeUpdate(sql);
      } catch ( SQLException sqlEx) {
        sqlEx.printStackTrace();
        throw new RuntimeException("Unable to create Source table", sqlEx);
      } finally {
        DBHelper.close(addSrcTabStmt);
      }
    }

    public void populateTable(Connection conn, int numRows, SeedTester tester)
    {
      String sql = tester.getInsertCmd();

      System.out.println("Command is :" + sql);

      PreparedStatement stmt = null;
      try
      {
        stmt =  conn.prepareStatement(sql);

        for ( int i = 0 ; i < numRows; i++)
        {
           tester.prepareRecord(stmt);
           stmt.executeUpdate();
           stmt.clearParameters();
        }
        conn.commit();
      } catch (SQLException sqlEx) {
        sqlEx.printStackTrace();
        throw new RuntimeException("Got exception when inserting records",sqlEx);
      } finally {
        DBHelper.close(stmt);
      }
    }

    public void runSeeder(final String[] args)
    {

      Runnable seeder = new Runnable() {
        @Override
        public void run() {
          try
          {
            BootstrapSeederMain.main(args);
          }
          catch (Exception e)
          {
            // TODO Auto-generated catch block
            e.printStackTrace();
            throw new RuntimeException("Got exception while running Seeder", e);
          }
        }
      };

      Thread seederThread = new Thread(seeder);
      seederThread.start();
      try
      {
        seederThread.join();
      } catch (InterruptedException ie) {
        throw new RuntimeException("Got interrupted while waiting for seeding to be done", ie);
      }
    }


	public static interface SeedTester
	{
	   public String getSrcTableName();

	   public int getSrcId();

	   public boolean isNumericKey();

	   public String  getPrimaryKeyName();

	   public String getSourcesConfigFile();

	   public String getCreateCmd();

	   public String getInsertCmd();

	   public void  prepareRecord(PreparedStatement stmt);

	   public void compareRecord(ResultSet exp, DbusEvent actual)
	        throws AssertionError;
	}


	public static class BizFollowTester
	     implements SeedTester
	{
	  private int _id  = 1;

	  @Override
    public String getSourcesConfigFile()
	  {
	    return "integration-test/config/sources-bizfollow.json";
	  }

	  @Override
    public boolean isNumericKey()
	  {
		  return true;
	  }

	  @Override
    public void prepareRecord(PreparedStatement stmt)
	  {
	    try
	    {
	      int id = _id++; //Records are inserted in the order of primaryKey
	      int member_id = RngUtils.randomPositiveInt();
	      int company_id = RngUtils.randomPositiveInt();
	      int notify_nus = RngUtils.randomPositiveInt();
	      int source = getSrcId();
	      String status = RngUtils.randomString(1);

	      stmt.setInt(1,id);
	      stmt.setInt(2,member_id);
	      stmt.setInt(3,company_id);
	      stmt.setInt(4,notify_nus);
	      stmt.setInt(5,source);
	      stmt.setString(6,status);

	    } catch (SQLException sqlEx) {
	      sqlEx.printStackTrace();
	      throw new RuntimeException("Got exception when preparing for inserting records",sqlEx);
	    }
	  }

	  @Override
    public void compareRecord(ResultSet exp, DbusEvent actual)
        throws AssertionError
	  {
	    try
	    {
	      VersionedSchemaSet schemaSet = new VersionedSchemaSet();
	      //TODO High use a real version here
	      schemaSet.add(new VersionedSchema(BizFollow.SCHEMA$.getFullName(), (short)1, BizFollow.SCHEMA$));
	      DbusEventAvroDecoder decoder = new DbusEventAvroDecoder(schemaSet);
	      BizFollow bizFollow = decoder.getTypedValue(actual, null, BizFollow.class);

	      Integer txn = bizFollow.get(0) == null ? 0 : (Integer)bizFollow.get(0);
	      Integer id = bizFollow.get(1) == null ? 0 : (Integer)bizFollow.get(1)  ;
	      Integer memberId = bizFollow.get(2) == null ? 0 : (Integer)bizFollow.get(2);
	      Integer companyId = bizFollow.get(3) == null ? 0 : (Integer)bizFollow.get(3);
	      String notifyOptions = (String) bizFollow.get(4);
	      Integer notifyNus = bizFollow.get(5) == null ? 0 : (Integer)bizFollow.get(5);
	      Integer source = bizFollow.get(6) == null ? 0 : (Integer)bizFollow.get(6);
	      CharSequence status = (CharSequence) bizFollow.get(7);
	      Long createdOn =  bizFollow.get(8) == null ? 0 : (Long) bizFollow.get(8);
	      Long lastModified = bizFollow.get(9) == null ? 0 : (Long) bizFollow.get(9);
	      Assert.assertEquals(exp.getInt(1),txn.intValue(), "TXN check");
	      Assert.assertEquals( exp.getInt(2),id.intValue(), "ID check");
	      Assert.assertEquals(exp.getInt(3),memberId.intValue(), "Member ID check");
	      Assert.assertEquals(exp.getInt(4),companyId.intValue(), "Company ID check");
	      Assert.assertEquals( exp.getString(5),notifyOptions, "Notify Options check");
	      Assert.assertEquals(exp.getInt(6),notifyNus.intValue(),"Notify NUS check");
	      Assert.assertEquals(exp.getInt(7),source.intValue(), "Source check");
	      Assert.assertEquals(exp.getString(8),new StringBuilder(status).toString(), "Status check" );
	      Assert.assertEquals(exp.getLong(9),createdOn.longValue(), "Created On check");
	      Assert.assertEquals(exp.getLong(10),lastModified.longValue(), "Last Modified check");

	    } catch ( SQLException sqlEx) {
	      sqlEx.printStackTrace();
	      throw new RuntimeException("SQLException while checking results", sqlEx);
	    }
	  }

	  @Override
    public String getPrimaryKeyName()
	  {
		  return "id";
	  }

	  @Override
    public String getSrcTableName()
	  {
	    return "sy$biz_follow_test";
	  }


	  @Override
    public int getSrcId()
	  {
	    return 1000;
	  }

	  @Override
    public String getCreateCmd()
	  {
	    String tableName = getSrcTableName();
	    StringBuilder sql = new StringBuilder();
	    sql.append("create table ");
	    sql.append(tableName);
	    sql.append("( txn number,");
	    sql.append(" id number NOT NULL,");
	    sql.append(" member_id number NOT NULL,");
	    sql.append(" company_id number NOT NULL,");
	    sql.append(" notify_options clob,");
	    sql.append(" notify_nus number NOT NULL,");
	    sql.append(" source number NOT NULL,");
	    sql.append(" status char(1) NOT NULL,");
	    sql.append(" created_on date,");
	    sql.append(" last_modified date)");
	    return sql.toString();
	  }

	  @Override
    public String getInsertCmd()
	  {
        String tableName = getSrcTableName();
        StringBuilder sql = new StringBuilder();
        sql.append("insert into ");
        sql.append(tableName);
        sql.append(" (id,member_id,company_id,notify_nus,source,status)");
        sql.append(" values(?,?,?,?,?,?)");
        return sql.toString();
	  }

	}

}
