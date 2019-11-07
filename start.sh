# #! /bin/bash 

cat "./server/${1}" | \
while read CMD 
do 
    YEAR_ENV="${CMD:0:4}"
    MONTH_ENV="${CMD:5:7}"
    TOPIC_HEADER="${YEAR_ENV}_${MONTH_ENV}"
    
    echo "[PREPARE] process data for ${YEAR_ENV}/${MONTH_ENV}"
    sed "s/%YEAR_ENV%/${YEAR_ENV}/g" env.template | tee env
    sed -i "s/%MONTH_ENV%/${MONTH_ENV}/g" env

    echo "[START] docker-compose"
    docker-compose up -d 

    echo "[CHECK]: check container's status"
    while :
    do
        if [[ -z $(docker ps -q) ]]; then
            break     
        fi
        sleep 300
    done 

    echo "[CLEAN UP]: delete topic" 
    A=$(~/kafka_2.12-2.3.0/bin/kafka-topics.sh --zookeeper localhost:2181 --list)
    for i in $A; do
        if [[ $i == "${TOPIC_HEADER}_"* ]]; then
            ~/kafka_2.12-2.3.0/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic $i
        fi
    done 

    echo "[CLEAN UP]: delete container"
    A=$(docker-compose ps -q)
    for i in $A; do 
        docker rm $i
    done  

done
