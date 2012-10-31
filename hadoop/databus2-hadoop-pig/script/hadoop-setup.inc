cd ${root_dir}

export HADOOP_HOME=$PWD/lib/hadoop
if [ ! -d ${HADOOP_HOME} ] ; then
  tar -zxf ${lib_dir}/hadoop.tgz -C ${lib_dir}
  if [ ! -d ${HADOOP_HOME} ] ; then
    echo "$0: unable to create hadoop dir"
    exit 1
  fi
fi

export PIG_HOME=$PWD/lib/pig
if [ ! -d ${PIG_HOME} ] ; then
  tar -zxf ${lib_dir}/pig.tgz -C ${lib_dir}
  if [ ! -d ${PIG_HOME} ] ; then
    echo "$0: unable to create pig dir"
    exit 1
  fi
fi

export HADOOP_PID_DIR=$PWD/logs

export PATH=$HADOOP_HOME/bin:$PIG_HOME/bin:$PATH:

function last_dir(){
  hadoop fs -ls $1 | grep $1/[0-9] | tail -1 | sed -e 's|.*'$1'\([0-9]*\)|\1|'
}