#allison:this is the standard start Hive Server script
#default port is 50000

port=${1:-50000}
server_home=$(cd "$(dirname $0)/../" && pwd)

cd "$server_home" && nohup bin/hive --service hiveserver $port &

