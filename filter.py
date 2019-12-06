import sys
import configparser

header = sys.argv[1]

config = configparser.ConfigParser()
config.read('/app/config.ini')

maxlen = int(config['ZombieDetector']['WaitFor']) // int(config['DEFAULT']['Interval']) 
maxlen += 5

for i in range(int(config['BGPScheduler']['PartitionNumber'])) :
    f = open(f'./data/zombies/{header}-zombieDetector-{i}.txt', 'r')
    fw = open(f'./data/zombies/{header}-zombieDetector-{i}-filtered.txt', 'w+')    
    
    line = f.readline()
    while line :
        deque = line.split('|')[-1]
        queue = deque.split("[")[1].split("]")[0]
        arr = list(map(int, queue.split(",")))
        
        check = False
        for i in range(maxlen) :
            if arr[-maxlen+i] <= arr[-maxlen]//2 :
                check = True 
            if arr[-maxlen+i] > arr[-maxlen]//2 and check : 
                check = False
                break

        if check and arr[0] >= arr[1] // 2 : 
            fw.write(f"{line}")
            
        line = f.readline()
    
    f.close()
    fw.close()