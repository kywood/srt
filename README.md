# srt
starrocks 테스트

##  datalake 구축

```

cd datalake
docker-compose up -d

 docker ps -a
CONTAINER ID   IMAGE                         COMMAND                  CREATED          STATUS                    PORTS                                                                                                                                   NAMES
86a099eb4a18   starrocks/be-ubuntu:latest    "/opt/starrocks/be/b…"   39 minutes ago   Up 39 minutes             0.0.0.0:8040->8040/tcp, [::]:8040->8040/tcp, 0.0.0.0:9050->9050/tcp, [::]:9050->9050/tcp                                                starrocks-be
a4ac83d8bee6   apache/kudu:latest            "/kudu-entrypoint.sh…"   39 minutes ago   Up 39 minutes             0.0.0.0:7051->7051/tcp, [::]:7051->7051/tcp, 0.0.0.0:8051->8051/tcp, [::]:8051->8051/tcp                                                kudu-tserver
e0a0a783717d   projectnessie/nessie:latest   "/usr/local/s2i/run"     39 minutes ago   Up 39 minutes             8080/tcp, 8443/tcp, 0.0.0.0:19120->19120/tcp, [::]:19120->19120/tcp                                                                     nessie
060e82c39d6c   starrocks/fe-ubuntu:latest    "/opt/starrocks/fe/b…"   39 minutes ago   Up 39 minutes             0.0.0.0:8030->8030/tcp, [::]:8030->8030/tcp, 0.0.0.0:9020->9020/tcp, [::]:9020->9020/tcp, 0.0.0.0:9030->9030/tcp, [::]:9030->9030/tcp   starrocks-fe
1961862e32e1   apache/spark:3.5.0            "/opt/spark/bin/spar…"   39 minutes ago   Up 39 minutes             0.0.0.0:7077->7077/tcp, [::]:7077->7077/tcp, 0.0.0.0:8080->8080/tcp, [::]:8080->8080/tcp                                                spark
667405dabe39   apache/kudu:latest            "/kudu-entrypoint.sh…"   39 minutes ago   Up 39 minutes             0.0.0.0:7050->7050/tcp, [::]:7050->7050/tcp, 0.0.0.0:8050->8050/tcp, [::]:8050->8050/tcp                                                kudu-master
b186d2a90c61   minio/minio:latest            "/usr/bin/docker-ent…"   39 minutes ago   Up 39 minutes (healthy)   0.0.0.0:9000-9001->9000-9001/tcp, [::]:9000-9001->9000-9001/tcp 

```


## 접속 정보
![img.png](img.png)


## 셋업

디비 베어 접속후

```
ALTER SYSTEM ADD BACKEND "starrocks-be:9050";
SHOW BACKENDS;


CREATE DATABASE IF NOT EXISTS herb24;

USE herb24;


CREATE TABLE detection_logs (
    detect_id VARCHAR(50),
    user_id INT,
    herb_name VARCHAR(50),
    confidence DOUBLE,
    latitude DOUBLE,
    longitude DOUBLE,
    device_os VARCHAR(20),
    detect_time DATETIME
)
DISTRIBUTED BY HASH(detect_id) BUCKETS 3
PROPERTIES(
    "replication_num" = "1"
);
```

## 실행
1. dataGenerator.py 실행
2. uploadData_StarrocksStreamLoadApi.py 실행  10만건의 데이터가 starrocks 에 저장됨





