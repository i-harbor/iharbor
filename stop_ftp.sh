ps aux | grep [h]arbor_ftp |awk '{print $2}' |xargs -r kill -9
