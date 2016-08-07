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
  if(len(data) == 0):
      # try eating everything
      for line in open(file):
        row = line.split()
        data.append(row)
  return data

def percent(bef,aft):
    diff = float(aft) - float(bef)
    diffrel = diff / float(bef)
    diffrelper = int(round(diffrel * 100))
    return diffrelper

def process(logfile1, logfile2, threshold,htmlfile):
  print("loading before data: "+logfile1)
  data1 = extractmap(logfile1)
  print("loading after data: "+logfile2)
  data2 = extractmap(logfile2)
  print("applying threshold: "+str(threshold))
  htmlout = open(htmlfile, "w")
  htmlout.write("<html><body>\n");
  htmlout.write("<table>\n");
  htmlout.write("<tr><th>test method</th><th>dataset</th><th>memory-mapped</th><th>format</th><th>time before</th><th>time after</th><th>percentage change</th></tr>\n");
  for row1,row2 in zip(data1,data2):
    assert row1[:4] == row2[:4]
    r = row1[:4]
    diffrelper = percent(row1[6],row2[6])
    if(abs(diffrelper) >= threshold):
      ratstr = ""
      if(diffrelper > 0):
          rat = round(float(row2[6]) /  float(row1[6]))
          if(rat >= 2):
            ratstr = "or x"+str(rat)
      else :
          rat = round(float(row1[6]) /  float(row2[6]))
          if(rat >= 2):
            ratstr = "or /"+str(rat)
      r.append(row1[6])
      r.append(row2[6])
      r.append(str(diffrelper)+"%")
      print(" ".join(r) )
      color='red'
      if(diffrelper < 0): color='blue'
      if(len(ratstr)==0): color='black'
      htmlout.write("<tr><td>{r[0]}</td><td>{r[1]}</td><td>{r[2]}</td><td>{r[3]}</td><td>{r[4]}</td><td>{r[5]}</td><td><span style='color:{color}'>{r[6]} {ratstr}</span></td></tr>\n".format(r=r,color=color,ratstr=ratstr));
  htmlout.write("</table>\n");
  htmlout.write("</body></html>\n");
  htmlout.close();
  print("see html report: "+htmlfile)

if __name__ == "__main__":
  if(len(sys.argv)<=2):
    print("Usage: compare.py logfile1 logfile2")
  if(len(sys.argv)>2):
     print("before:"+sys.argv[1]+ " after: " +sys.argv[2])
     threshold = 5
     if(len(sys.argv) > 3) :
         threshold = int(sys.argv[3])
     process(sys.argv[1],sys.argv[2], threshold, "diff.html")
