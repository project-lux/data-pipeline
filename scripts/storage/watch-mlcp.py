
import re
import time
import datetime
import shutil
import asciichartpy as acp
# 24/05/13 19:57:56 INFO contentpump.LocalJobRunner:  completed 0%

WARNING_TIME = 240
offset = datetime.timedelta(hours=4)
line_parser = re.compile('([0-9/]+) ([0-9:]+) INFO contentpump.LocalJobRunner:\s+completed ([0-9%]+)')

fh = open('nohup.out')
finished = False
percents = []

while not finished:
    lines = fh.readlines()
    for l in lines:
        if "completed" in l:
            m = line_parser.match(l)
            (d, t, p) = m.groups()
            (yy,mm,dd) = d.split('/')
            dts = f"20{yy}-{mm}-{dd}T{t}"
            dt = datetime.datetime.fromisoformat(dts)
            percents.append((dt, p))
            if len(percents) > 1:
                prev = percents[-1][0]
                diff = dt - prev
                if diff.seconds > WARNING_TIME:
                    print(f" *** Took {diff.seconds} seconds for mark {p} ***")

                x = list(range(len(percents)))
                y = []
                ts = shutil.get_terminal_size()
                lines = ts.lines
                for idx in range(1,len(percents)):
                    diff = percents[idx][0]-percents[idx-1][0]
                    y.append(diff.seconds)
                maxes = [WARNING_TIME]*len(percents)
                running = []
                for idx in range(1,len(percents)):
                    running.append(sum(y[:idx])/idx)
                print(acp.plot([y, maxes, running], {'height':lines-6, 'colors':[acp.green, acp.red, acp.yellow]}))
                print(f"{' '*11}{''.join([str(x)*10 for x in range(10)])}")
                print(f"{' '*11}{''.join([str(x) for x in range(10)])*10}")


                pint = int(p.strip()[:-1])
                left = 100-pint
                expected = left * running[-1]
                when = datetime.datetime.now() - offset + datetime.timedelta(seconds=expected)
                print(f"Key: Green=Seconds/Percent;Yellow=Average;Red=Too Slow || Done {p}: finish at average rate: {when.isoformat()}")


            if p == "100%":
                finished = True
    time.sleep(60)
