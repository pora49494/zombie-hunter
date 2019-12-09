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
        python3 /app/bgpScheduler.py -s $START -e $END -c all 
        ;;

    detector)
        echo "detector"
        sleep 120
        python3 /app/zombieDetector.py -s $START -e $END -p $partition 
        ;;
    
    cleaner) 
        echo "cleaner"
        python3 /app/clean_up.py $TOPIC_HEADER
        ;;
esac
