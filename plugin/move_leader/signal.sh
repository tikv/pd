pid=`ps -ef|grep -E pd-server | grep -v "grep" | awk '{print $2}'`
kill -SIGUSR1 $pid
