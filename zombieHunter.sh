#! /bin/bash

case $JOB in
    producer) 
        for i in `seq -f "%02g" 0 23` ; do
            python3 /app/bgpProducer.py -s $START -e $END -c rrc${i} -t ribs &
            python3 /app/bgpProducer.py -s $START -e $END -c rrc${i} -t updates &
        done
        # python3 /app/bgpProducer.py -s $START -e $END -c rrc00 -t ribs &
        # python3 /app/bgpProducer.py -s $START -e $END -c rrc00 -t updates &
        # python3 /app/bgpProducer.py -s $START -e $END -c rrc01 -t ribs &
        # python3 /app/bgpProducer.py -s $START -e $END -c rrc01 -t updates &
        # python3 /app/bgpProducer.py -s $START -e $END -c rrc04 -t ribs &
        # python3 /app/bgpProducer.py -s $START -e $END -c rrc04 -t updates &
        ;;

    analyzer)
        sleep 10
        for i in `seq -f "%02g" 0 23` ; do
            python3 /app/bgpAnalyzer.py -s $START -e $END -c rrc${i} &
        done
        # python3 /app/bgpAnalyzer.py -s $START -e $END -c rrc00  &
        # python3 /app/bgpAnalyzer.py -s $START -e $END -c rrc01  &
        # python3 /app/bgpAnalyzer.py -s $START -e $END -c rrc04  &
        ;;

    scheduler)
        sleep 20
        python3 /app/bgpScheduler.py -s $START -e $END -c all &
        # python3 /app/bgpScheduler.py -s $START -e $END -c rrc00,rrc01,rrc04 &
        ;;

    detector)
        sleep 30
        for i in `seq 0 9` ; do 
            python3 /app/zombieDetector.py -s $START -e $END -p $i &
        done 
    ;;
esac

while : 
do
    sleep 300
    pgrep python3 || exit 0
done