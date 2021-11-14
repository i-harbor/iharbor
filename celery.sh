#!/usr/bin/env bash
#
#syncserver/celery.sh
#
#Start/stop/restart/ the celery worker
#
#Usage: ./celery.sh start|stop|restart

# set your -c and multi parames here
concurrency=4
cores=$(cat /proc/cpuinfo| grep "processor"| wc -l)
celery_pid="/var/run/celery/celery1.pid"
ipaddr=$(ifconfig -a|grep inet|grep -v 127.0.0.1|grep -v 172.17.0.1|grep -v inet6|awk '{print $2}'|tr -d "addr:")

celery_start() {
  mkdir -p './syncserver/log'
  if [ -f "$celery_pid" ]; then
    echo "Already start, please try to use restart to reload"
  else
    celery multi start $cores -A syncserver -P gevent -c $concurrency -l info -n "$ipaddr"
  fi
}
celery_stop() {
  celery multi stop $cores -A syncserver -P gevent -c $concurrency -l info -n "$ipaddr"
  ps -ef | grep celery | grep -v grep | awk '{print $2}' | xargs kill -9
  rm -rf /var/log/celery
  rm -rf /run/celery
}

celery_restart() {
  if [ -f "$celery_pid" ]; then
    echo "stopping the celery"
    celery_stop
    echo "starting the celery"
    celery_start
    echo "done"
  else
    echo "celery is not running"
  fi
}

case "$1" in
'start')
  celery_start
  ;;
'stop')
  celery_stop
  ;;
'restart')
  celery_restart
  ;;
*)
  echo "usage $0 start|stop|restart"
  ;;
esac