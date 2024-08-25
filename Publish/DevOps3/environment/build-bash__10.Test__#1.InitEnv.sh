set -e


#---------------------------------------------------------------------
# args

args_="

export basePath=/root/temp

# "


#---------------------------------------------------------------------
echo '#build-bash__10.Test__#1.InitEnv.sh -> #1 start ClickHouse container'
docker rm vitorm-clickhouse -f || true
docker run -d \
--name vitorm-clickhouse \
--ulimit nofile=262144:262144 --privileged=true \
-p 8123:8123 -p 9000:9000 -p 9009:9009 \
clickhouse/clickhouse-server:22.2.3.5




#---------------------------------------------------------------------
echo '#build-bash__10.Test__#1.InitEnv.sh -> #8 wait for containers to init'


echo '#build-bash__10.Test__#1.InitEnv.sh -> #8.1 wait for ClickHouse to init' 
docker run -t --rm --link vitorm-clickhouse curlimages/curl sh -c 'until curl "http://vitorm-clickhouse:8123/?query=" -s --data-binary "SELECT 1"; do echo waiting for ClickHouse; sleep 2; done;    curl "http://vitorm-clickhouse:8123/?query=" -s --data-binary "create database if not exists db_orm;";    curl "http://vitorm-clickhouse:8123/?query=" -s --data-binary "create database if not exists db_orm2;";    curl "http://vitorm-clickhouse:8123/?query=" -s --data-binary "create database if not exists db_orm22;";'


#---------------------------------------------------------------------
echo '#build-bash__10.Test__#1.InitEnv.sh -> #9 init test environment success!'