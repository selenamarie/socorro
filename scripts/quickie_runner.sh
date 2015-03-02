#!/bin/bash

# integration test for Socorro, using rabbitmq
#
# bring up components, submit test crash, ensure that it shows up in
# reports tables.
#
# This uses the same setup as http://socorro.readthedocs.org/en/latest/installation.html

source scripts/defaults

if [ "$#" != "1" ] || [ "$1" != "--destroy" ]
then
  echo "WARNING - this script will destroy the local socorro install."
  echo "The default database and config files will be overwritten."
  echo "You must pass the --destroy flag to continue."
  exit 1
fi

function cleanup_rabbitmq() {
  echo -n "INFO: Purging rabbitmq queue 'socorro.normal'..."
  python scripts/test_rabbitmq.py --test_rabbitmq.purge='socorro.normal' --test_rabbitmq.rabbitmq_host=$rmq_host --test_rabbitmq.rabbitmq_user=$rmq_user --test_rabbitmq.rabbitmq_password=$rmq_password --test_rabbitmq.rabbitmq_vhost=$rmq_virtual_host > /dev/null 2>&1
  echo " Done."
  echo -n "INFO: Purging rabbitmq queue 'socorro.priority'..."
  python scripts/test_rabbitmq.py --test_rabbitmq.purge='socorro.priority' --test_rabbitmq.rabbitmq_host=$rmq_host --test_rabbitmq.rabbitmq_user=$rmq_user --test_rabbitmq.rabbitmq_password=$rmq_password --test_rabbitmq.rabbitmq_vhost=$rmq_virtual_host > /dev/null 2>&1
  echo " Done."
}

function cleanup() {
  cleanup_rabbitmq

  echo "INFO: cleaning up crash storage directories"
  rm -rf ./primaryCrashStore/ ./processedCrashStore/
  rm -rf ./crashes/

  echo "INFO: Terminating background jobs"

  for p in collector processor middleware
  do
    # destroy any running processes started by this shell
    kill `jobs -p`
    # destroy anything trying to write to the log files too
    fuser -k ${p}.log > /dev/null 2>&1
  done

  return 0
}

trap 'cleanup' INT

function fatal() {
  exit_code=$1
  message=$2

  echo "ERROR: $message"

  cleanup

  exit $exit_code
}

echo -n "INFO: setting up environment..."
source ${VIRTUAL_ENV:-"socorro-virtualenv"}/bin/activate >> setup.log 2>&1
if [ $? != 0 ]
then
  fatal 1 "could activate virtualenv"
fi
export PYTHONPATH=.
echo " Done."

# ensure rabbitmq is really empty and no previous failure left garbage
cleanup_rabbitmq

echo -n "INFO: setting up 'weekly-reports-partitions' via crontabber..."
python socorro/cron/crontabber_app.py --resource.postgresql.database_url=$database_url --job=weekly-reports-partitions --force >> setupdb.log 2>&1
if [ $? != 0 ]
then
  fatal 1 "crontabber weekly-reports-partitions failed, check setupdb.log"
fi
echo " Done."

echo -n "INFO: configuring backend jobs..."
for p in collector processor middleware
do
  cp config/${p}.ini-dist config/${p}.ini
  if [ $? != 0 ]
  then
    fatal 1 "copying default config for $p failed"
  fi
  # ensure no running processes
  fuser -k ${p}.log > /dev/null 2>&1
done
echo " Done."

echo -n "INFO: starting up collector, processor and middleware..."
python socorro/collector/collector_app.py --admin.conf=./config/collector.ini --resource.rabbitmq.host=$rmq_host --secrets.rabbitmq.rabbitmq_user=$rmq_user --secrets.rabbitmq.rabbitmq_password=$rmq_password --resource.rabbitmq.virtual_host=$rmq_virtual_host --resource.rabbitmq.transaction_executor_class=socorro.database.transaction_executor.TransactionExecutor --web_server.wsgi_server_class=socorro.webapi.servers.CherryPy > collector.log 2>&1 &
python socorro/processor/processor_app.py --admin.conf=./config/processor.ini --resource.rabbitmq.host=$rmq_host --secrets.rabbitmq.rabbitmq_user=$rmq_user --secrets.rabbitmq.rabbitmq_password=$rmq_password --resource.rabbitmq.virtual_host=$rmq_virtual_host --resource.postgresql.database_url=$database_url > processor.log 2>&1 &
sleep 1
python socorro/middleware/middleware_app.py --admin.conf=./config/middleware.ini --database.database_url=$database_url --rabbitmq.host=$rmq_host --rabbitmq.rabbitmq_user=$rmq_user --rabbitmq.rabbitmq_password=$rmq_password --rabbitmq.virtual_host=$rmq_virtual_host --web_server.wsgi_server_class=socorro.webapi.servers.CherryPy > middleware.log 2>&1 &
echo " Done."

function retry() {
  name=$1
  search=$2

  count=0
  while true
  do
    grep "$search" ${name}.log > /dev/null
    if [ $? != 0 ]
    then
      echo "INFO: waiting for $name..."
      if [ $count == 30 ]
      then
        cat $name.log
        fatal 1 "$name timeout"
      fi
    else
      grep 'ERROR' ${name}.log
      if [ $? != 1 ]
      then
        cat ${name}.log
        fatal 1 "errors found in $name.log"
      fi
      echo "INFO: $name test passed"
      break
    fi
    sleep 5
    count=$((count+1))
  done
  }

# wait for collector to startup
retry 'collector' 'running standalone at 0.0.0.0:8882'

while true ; do
   sleep 5 
done
