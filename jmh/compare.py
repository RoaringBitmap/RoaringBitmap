#!/usr/bin/env python
import sys

def extractmap(file):
  endflag = False
  accumulate = False
  data = []
  for line in open(file):
    if(accumulate):
        row = line.split()
        data.append(row)
    if(line.startswith("# Run complete.")):
        endflag = True
    if (endflag and line.startswith("Benchmark ")):
        accumulate = True
  return data

def process(logfile1, logfile2):
  data1 = extractmap(logfile1)
  data2 = extractmap(logfile2)
  for row1,row2 in zip(data1,data2):
    r = row1[:]
    r[6] = "\t"+row1[6]+"\t"+row2[6]+"\t"+str(int(round((float(row2[6])-float(row1[6]))*100./float(row1[6]))))+"%\t"
    print(" ".join(r) )


if __name__ == "__main__":
  if(len(sys.argv)<=2):
    print("Usage: compare.py logfile1 logfile2")
  if(len(sys.argv)>2):
     process(sys.argv[1],sys.argv[2])
