num_steps=1

######################## LOGGING ###########################
# log level constants
LOGLVL_NONE=5
LOGLVL_ERROR=4
LOGLVL_WARN=3
LOGLVL_INFO=2
LOGLVL_DEBUG=1
LOGLVL_ALL=0

declare -a LOGLVL_STR=(ALL DEBG INFO WARN ERRO NONE)

export LOG_LEVEL=$LOGLVL_INFO
export TSTAMP_FORMAT="%F %T.%N"

function LOG_ERROR() {
  LOG $LOGLVL_ERROR $*
}

function LOG_WARN() {
  LOG $LOGLVL_WARN $*
}

function LOG_INFO() {
  LOG $LOGLVL_INFO $*
}

function LOG_DEBUG() {
  LOG $LOGLVL_DEBUG $*
}

function LOG() {
  level=$1
  shift
  msg="$*"
  
  level_str="${LOGLVL_STR[$level]}"
  
  if [ $level -ge $LOG_LEVEL ] ; then
    d=`date +"$TSTAMP_FORMAT"`
    echo "$d $level_str ----------- $msg ----------- "
  fi
}

###################### TESTS ##################################

function TEST_STEP() {
  export stat_txt="$*"
  
  d=`date +"$TSTAMP_FORMAT"`
  echo "$d ============== STEP $num_steps : $stat_txt =============="
}

function REPORT_TEST_STEP() {
  stat=$?
  d=`date +"$TSTAMP_FORMAT"`
  if [ $stat == 0 ]; then
    result=SUCCESS
  else
    result=FAIL
    all_stat=$((all_stat+1))
  fi
  
  echo "$d ============== STEP $num_steps : $stat_txt : $result =============="  
  num_steps=$((num_steps+1))
}

function FINAL_TEST_REPORT() {
   echo "######################################################################"
   if [ $all_stat -gt 0 ]; then
     if [ $all_stat -gt 1 ]; then
       echo "## $0: FAIL. There are $all_stat failing steps."
     else
       echo "## $0: FAIL. There is 1 failing step."
     fi
   else
     if [ $num_steps -gt 1 ]; then
       echo "## $0: ALL $num_steps steps for test finished with status SUCCESS."
     fi
   fi
   echo "######################################################################"
}

###################### FILES ###################################
# file utility functions
function CREATE_CLEAN_DIR(){
  dir=$1
  mkdir -p $dir
  rm -rf $dir/*
}
