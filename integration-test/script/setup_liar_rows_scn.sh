usage() {
  echo "Usage: $0 <username/password[@SID]> [load|unload|update|updatescn] <start_id> <end_id> [source]"
}

# Database Connection Info
DB=$1

#CMD
CMD=`echo $2 | tr '[A-Z]' '[a-z]'`

# start Primary Key
start_id=$3

# start Primary Key
end_id=$4

#SOURCE
src=`echo $5 | tr '[A-Z]' '[a-z]'`

if [ "x$CMD" = "x" ] || [ "x$DB" = "x" ]
then
  usage
  exit 1
fi

if [ "$CMD" != "load" ] && [ "$CMD" != "unload" ] && [ "$CMD" != "updatescn" ] && [ "$CMD" != "update" ]
then
  usage
  exit 1
fi

if [ "$CMD" = "load" ] || [ "$CMD" = "update" ]
then
  if [ "x$src" = "x" ] || [ "x$start_id" = "x" ] || [ "x$end_id" = "x" ]
  then
    usage
    exit 1
  fi
  if [ "$src" != "job" ] && [ "$src" != "member" ] && [ "$src" != "news" ] && [ "$src" != "creative" ]
  then
    echo "Source Names should be one of 'job', 'news','member','creative'";
    exit 1
  fi 
elif [ "$CMD" = "updatescn" ]
then
   if [ "x$start_id" = "x" ] || [ "s$end_id" = "x" ] 
  then
    usage
    exit 1
  fi
fi

echo "INFO: define and install load data routines"
sqlplus $DB << __EOF__
CREATE OR REPLACE PACKAGE TESTLIAR AS 
  Procedure loadTestLiarJob( s number, e number);
  Procedure loadTestLiarMember( s number, e number);
  Procedure loadTestLiarNews( s number, e number);
  Procedure loadTestLiarCreative( s number, e number);
  Procedure updateTestLiarJob(s number, e number);
  Procedure updateTestLiarMember(s number, e number);
  Procedure updateTestLiarNews(s number, e number);
  Procedure updateTestLiarCreative(s number, e number);
  Procedure updateScn( scn number, start_row number );
  Procedure unloadTestLiar;
End TESTLIAR;
/
Create or Replace Package Body TESTLIAR as
Procedure loadTestLiarJob(s number, e number) is
begin
  dbms_output.put_line('start inserting liar_job_relay data');
  for cntr IN s..e LOOP
    dbms_output.put_line(cntr);
    insert into liar_job_relay(event_id) values(cntr);
    commit;
  END LOOP;
  dbms_output.put_line('completed inserting liar_job_relay data');
end loadTestLiarJob;

Procedure loadTestLiarMember(s number, e number) is
begin
  dbms_output.put_line('start inserting liar_member_relay data');
  for cntr IN s..e LOOP
    dbms_output.put_line(cntr);
    insert into liar_member_relay(event_id) values(cntr);
    commit;
  END LOOP;
  DBMS_SCHEDULER.run_job('J_COALESCE_LOG');
  dbms_output.put_line('completed inserting liar_member_relay data');
end loadTestLiarMember;

Procedure loadTestLiarNews(s number, e number) is
begin
  dbms_output.put_line('start inserting liar_news_relay data');
  for cntr IN s..e LOOP
    dbms_output.put_line(cntr);
    insert into liar_news_relay(event_id) values(cntr);
    commit;
  END LOOP;
  dbms_output.put_line('completed inserting liar_news_relay data');
end loadTestLiarNews;

Procedure loadTestLiarCreative(s number, e number) is
begin
  dbms_output.put_line('start inserting liar_creative_relay data');
  for cntr IN s..e LOOP
    dbms_output.put_line(cntr);
    insert into liar_creative_relay(event_id) values(cntr);
    commit;
  END LOOP;
  DBMS_SCHEDULER.run_job('J_COALESCE_LOG');
  dbms_output.put_line('completed inserting liar_creative_relay data');
end loadTestLiarCreative;

Procedure updateTestLiarJob(s number, e number) is
begin
  dbms_output.put_line('updating liar_job_relay data');
  for i in ( select * from liar_job_relay where event_id >= s and event_id <= e) 
  LOOP
    if ( i.is_delete = 'false' ) then
      update liar_job_relay set is_delete = 'true' where event_id = i.event_id;
    else
      update liar_job_relay set is_delete = 'false' where event_id = i.event_id;
    end if;
    commit;
  END LOOP;
  dbms_output.put_line('completed updating liar_job_relay data');
end updateTestLiarJob;

Procedure updateTestLiarMember(s number, e number) is
begin
  dbms_output.put_line('updating liar_member_relay data');
  for i in ( select * from liar_member_relay where event_id >= s and event_id <= e) 
  LOOP
    if ( i.is_delete = 'false' ) then
      update liar_member_relay set is_delete = 'true' where event_id = i.event_id;
    else
      update liar_member_relay set is_delete = 'false' where event_id = i.event_id;
    end if;
    commit;
  END LOOP;
  dbms_output.put_line('completed updating liar_member_relay data');
end updateTestLiarMember;

Procedure updateTestLiarNews(s number, e number) is
begin
  dbms_output.put_line('updating liar_news_relay data');
  for i in ( select * from liar_news_relay where event_id >= s and event_id <= e) 
  LOOP
    if ( i.is_delete = 'false' ) then
      update liar_news_relay set is_delete = 'true' where event_id = i.event_id;
    else
      update liar_news_relay set is_delete = 'false' where event_id = i.event_id;
    end if;
    commit;
  END LOOP;
  dbms_output.put_line('completed updating liar_news_relay data');
end updateTestLiarNews;

Procedure updateTestLiarCreative(s number, e number) is
begin
  dbms_output.put_line('updating liar_creative_relay data');
  for i in ( select * from liar_creative_relay where event_id >= s and event_id <= e) 
  LOOP
    if ( i.is_delete = 'false' ) then
      update liar_creative_relay set is_delete = 'true' where event_id = i.event_id;
    else
      update liar_creative_relay set is_delete = 'false' where event_id = i.event_id;
    end if;
    commit;
  END LOOP;
  dbms_output.put_line('completed updating liar_creatibe_relay data');
end updateTestLiarCreative;

-- Un-Loads Liar table rows from the database
Procedure updateScn(scn number, start_row number) is
st number(6) := scn;
st2 number(6) := start_row;
begin
  dbms_output.put_line('setup scns is in txlog');
  for i in ( select * from sy\$txlog order by txn)
  loop
    st2 := st2 - 1;
    if ( st2 <= 0)  then
      dbms_output.put_line('Old SCN Value is :' || i.scn );
      update sy\$txlog set scn = st where txn = i.txn;
      st := st + 1;
      dbms_output.put_line('New SCN Value is :' || st );
      commit;
    end if;
  end loop;
  dbms_output.put_line('done setting up scns in txlog');
end updateScn;

-- Un-Loads Liar table rows from the database
Procedure unloadTestLiar is
begin
  dbms_output.put_line('delete liar_job_relay rows');
  delete from liar_job_relay;
  dbms_output.put_line('delete liar_member_relay rows');
  delete from liar_member_relay;
  dbms_output.put_line('delete liar_news_relay rows');
  delete from liar_news_relay;
  dbms_output.put_line('delete liar_creative_relay rows');
  delete from liar_creative_relay;
  dbms_output.put_line('delete sy\$txlog rows');
  delete from sy\$txlog;
end unloadTestLiar;
End TESTLIAR;
/
show errors;
__EOF__

if [ "$CMD" = "load" ];
then
   echo "INFO: Call load for sourec $src"
   if [ "$src" = "job" ]
   then 
     sqlplus $DB << __EOF__
       set serveroutput on; 
       call TestLiar.loadTestLiarJob($start_id, $end_id);
__EOF__
   elif [ "$src" = "member" ] 
   then
     sqlplus $DB << __EOF__
       set serveroutput on; 
       call TestLiar.loadTestLiarMember($start_id, $end_id);
__EOF__
  elif [ "$src" = "news" ] 
  then
     sqlplus $DB << __EOF__
       set serveroutput on; 
       call TestLiar.loadTestLiarNews($start_id, $end_id);
__EOF__
  elif [ "$src" = "creative" ] 
  then
     sqlplus $DB << __EOF__
       set serveroutput on; 
       call TestLiar.loadTestLiarCreative($start_id, $end_id);
__EOF__
  fi
elif [ "$CMD" = "unload" ]
then
   echo "INFO: Call unload"
   sqlplus $DB << __EOF__
     set serveroutput on ;
     call TestLiar.unloadTestLiar();
__EOF__
elif [ "$CMD" = "update" ];
then
   echo "INFO: Call update for source $src"
   if [ "$src" = "job" ]
   then 
     sqlplus $DB << __EOF__
       set serveroutput on; 
       call TestLiar.updateTestLiarJob($start_id, $end_id);
__EOF__
   elif [ "$src" = "member" ] 
   then
     sqlplus $DB << __EOF__
       set serveroutput on; 
       call TestLiar.updateTestLiarMember($start_id, $end_id);
__EOF__
  elif [ "$src" = "news" ] 
  then
     sqlplus $DB << __EOF__
       set serveroutput on; 
       call TestLiar.updateTestLiarNews($start_id, $end_id);
__EOF__
  elif [ "$src" = "creative" ] 
  then
     sqlplus $DB << __EOF__
       set serveroutput on; 
       call TestLiar.updateTestLiarCreative($start_id, $end_id);
__EOF__
  fi
else 
   echo "setup scn called"
   sqlplus $DB << __EOF__
     set serveroutput on ;
     call TestLiar.updateScn($start_id, $end_id);
__EOF__
fi
