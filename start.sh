#! /bin/bash 

# docker build -t pora/bgpstream:latest ./bgpStream
# docker build -t pora/bgpconfluent:latest ./bgpConfluent

cat "./server/${1}" | \
while read CMD 
do 
    YEAR_ENV="${CMD:0:4}"
    MONTH_ENV="${CMD:5:7}"
    TOPIC_HEADER="${YEAR_ENV}_${MONTH_ENV}"
    
    echo "[PREPARE] process data for ${YEAR_ENV}/${MONTH_ENV}"
    sed "s/%YEAR_ENV%/${YEAR_ENV}/g" env.template | tee env
    sed -i "s/%MONTH_ENV%/${MONTH_ENV}/g" env
     
    echo "[RUN] run producer containers"
    for i in $(seq -w 0 23) ; do
        if [[ $i == "02" ]] || [[ $i == "08" ]] || [[ $i == "09" ]] || [[ $i == "17" ]] ; then 
            continue
        fi 
        
        docker run -d \
        --name "${TOPIC_HEADER}_producer_${i}" \
        --network host \
        -v "${PWD}"/data/logs:/app/logs \
        -v "${PWD}"/config.ini:/app/config.ini \
        -v "${PWD}"/zombieHunter.sh:/app/zombieHunter.sh \
        -e collector="rrc${i}" \
        -e JOB="producer" \
        --env-file env \
        pora/bgpstream:latest \
        /app/zombieHunter.sh
    done 

    echo "[RUN] run scheduler containers"
    docker run -d \
    --name "${TOPIC_HEADER}_scheduler" \
    --network host \
    -v "${PWD}"/data/logs:/app/logs \
    -v "${PWD}"/config.ini:/app/config.ini \
    -v "${PWD}"/zombieHunter.sh:/app/zombieHunter.sh \
    -e JOB="scheduler" \
    --env-file env \
    pora/bgpconfluent:latest \
    /app/zombieHunter.sh 

    echo "[RUN] run detector containers"
    for i in `seq 0 9` ; do         
        docker run -d \
        --name "${TOPIC_HEADER}_detector_${i}" \
        --network host \
        -v "${PWD}"/data/logs:/app/logs \
        -v "${PWD}"/data/zombies:/app/zombies \
        -v "${PWD}"/config.ini:/app/config.ini \
        -v "${PWD}"/zombieHunter.sh:/app/zombieHunter.sh \
        -e partition="$i" \
        -e JOB="detector" \
        --env-file env \
        pora/bgpconfluent:latest \
        /app/zombieHunter.sh 
    done    

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
    A=$(docker ps -qa)
    for i in $A; do 
        docker rm $i
    done  

done
