#! /bin/bash

case $JOB in
    producer)
        echo "producer"
        python3 /app/bgpProducer.py -s $START -e $END -c $collector -t ribs &
        python3 /app/bgpProducer.py -s $START -e $END -c $collector -t updates &
        sleep 15
        python3 /app/bgpAnalyzer.py -s $START -e $END -c $collector 
        ;;

    scheduler)
        echo "scheduler"
        sleep 30
        python3 /app/bgpScheduler.py -s $START -e $END -c all 
        ;;

    detector)
        echo "detector"
        sleep 60
        python3 /app/zombieDetector.py -s $START -e $END -p $partition 
        ;;
esac
