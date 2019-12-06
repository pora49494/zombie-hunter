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
    for i in $(seq -w 0 21) ; do
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
    -v "${PWD}"/data/buf:/app/buf \
    -v "${PWD}"/config.ini:/app/config.ini \
    -v "${PWD}"/zombieHunter.sh:/app/zombieHunter.sh \
    -e JOB="scheduler" \
    -e START=${YEAR_ENV}-${MONTH_ENV}-10T00:00:00 \
    -e END=${YEAR_ENV}-${MONTH_ENV}-20T00:00:00 \
    pora/bgpconfluent:latest \
    /app/zombieHunter.sh 

    echo "[RUN] run detector containers"
    for i in `seq 0 9` ; do         
        docker run -d \
        --name "${TOPIC_HEADER}_detector_${i}" \
        --cpus="2" \
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

    echo "[FILE MANAGEMENT]: saving data"
    sudo chown pora:pora data
    sudo chown pora:pora data/*
    sudo chown pora:pora data/logs/*
    sudo chown pora:pora data/zombies/*
    
    month=$(expr MONTH_ENV + 0)
    CUR=$(pwd)

    cd ${CUR}/data/logs/
    tar -czf ${YEAR_ENV}-${month}-logs.tar.bz ${YEAR_ENV}-${month}-*
    scp ${YEAR_ENV}-${month}-logs.tar.bz pora-2:~/archive/logs/
    mv ${YEAR_ENV}-${month}-logs.tar.bz ${CUR}/archive/

    cd ${CUR}
    python3 filter.py ${YEAR_ENV}-${month}
    
    cd ${CUR}/data/zombies/
    tar -czf ${YEAR_ENV}-${month}-zombieHunter.tar.bz ${YEAR_ENV}-${month}-*
    scp ${YEAR_ENV}-${month}-zombieHunter.tar.bz pora-2:~/archive/zombies/
    mv ${YEAR_ENV}-${month}-zombieHunter.tar.bz ${CUR}/archive/

    echo "[CLEAN UP]: delete container"
    A=$(docker ps -qaf "name=${TOPIC_HEADER}_")
    for i in $A; do 
        docker rm $i
    done  

    cd ${CUR}
    
    sleep 300
    
done
