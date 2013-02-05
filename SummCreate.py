import simplejson
import math
import DQXMathUtils
import os
import re
import DQXEncoder
import random


#############################################################################################


def FilterValuesToFloat(list):
    rs=[]
    for vl in list:
        if vl!=None:
            if vl!='\N':
                rs.append(float(vl))
    return rs

class ValueList:
    def __init__(self,ilist):
        self.list=ilist
        self.floatlist=None
        self.sortedfloatlist=None
    def GetOrigList(self):
        return self.list
    def GetFloatList(self):
        if not(self.floatlist):
            self.floatlist=FilterValuesToFloat(self.list)
        return self.floatlist
    def GetSortedFloatList(self):
        if not(self.sortedfloatlist):
            self.sortedfloatlist=sorted(self.GetFloatList())
        return self.sortedfloatlist




class Summariser:
    def __init__(self,info):
        self.propID=info['PropID']
        self.IDExt=info['IDExt']
        self.ID=self.propID+'_'+self.IDExt
        self.encoder=DQXEncoder.GetEncoder(info['Encoder'])
    def calcSummary(self,list):
        raise Exception('Summary function not implemented')
    def Perform(self,list):#list is of class ValueList
        summvalue=self.calcSummary(list)
        rs=self.encoder.perform(summvalue)
        if len(rs)!=self.encoder.getlength():
            raise Exception('Encoder returned wrong size')
        return rs



class SummariserPickRandom(Summariser):
    def __init__(self,info):
        Summariser.__init__(self,info)
    def calcSummary(self,list):
        lst=list.GetOrigList()
        if len(lst)==0:
            return None
        else:
            idx=int(random.randint(0,len(lst)-1))
            return lst[idx]
    def getInfo(self):
        return {'ID':'PickRandom' }


class SummariserMostFrequent(Summariser):
    def __init__(self,info):
        Summariser.__init__(self,info)
    def calcSummary(self,list):
        lst=list.GetOrigList()
        if len(lst)==0:
            return None
        else:
            freqdict={}
            for item in lst:
                if not(item in freqdict):
                    freqdict[item]=0
                freqdict[item]+=1
            maxfreq=0
            mostfrequentitemlist=[]
            for item in freqdict:
                if freqdict[item]>maxfreq:
                    maxfreq=freqdict[item]
                    mostfrequentitemlist=[]
                if freqdict[item]==maxfreq:
                    mostfrequentitemlist.append(item)
            mostfrequentitem=mostfrequentitemlist[random.randint(0,len(mostfrequentitemlist)-1)]
#            if len(lst)>2:
#                print('----------------')
#                print(str(freqdict))
#                print(str(mostfrequentitemlist))
#                print(str(mostfrequentitem))
            return mostfrequentitem
    def getInfo(self):
        return {'ID':'MostFrequent' }


class SummariserAverage(Summariser):
    def __init__(self,info):
        Summariser.__init__(self,info)
    def calcSummary(self,list):
        floatlist=list.GetFloatList()
        if len(floatlist)==0:
            return None
        return sum(floatlist)/float(len(floatlist))
    def getInfo(self):
        return {'ID':'Average' }

class SummariserMax(Summariser):
    def __init__(self,info):
        Summariser.__init__(self,info)
    def calcSummary(self,list):
        floatlist=list.GetFloatList()
        if len(floatlist)==0:
            return None
        return max(floatlist)
    def getInfo(self):
        return {'ID':'Max' }

class SummariserMin(Summariser):
    def __init__(self,info):
        Summariser.__init__(self,info)
    def calcSummary(self,list):
        floatlist=list.GetFloatList()
        if len(floatlist)==0:
            return None
        return min(floatlist)
    def getInfo(self):
        return {'ID':'Min' }

class SummariserQuantile(Summariser):
    def __init__(self,info):
        Summariser.__init__(self,info)
        self.frac=info['Fraction']
    def calcSummary(self,list):
        sortfloatlist=list.GetSortedFloatList()
        if len(sortfloatlist)==0:
            return None
        return DQXMathUtils.quantile(sortfloatlist,self.frac,7,True)
    def getInfo(self):
        return {'ID':'Quantile', 'Frac':self.frac }

class SummariserHistogram(Summariser):
    def __init__(self,info):
        Summariser.__init__(self,info)
        self.count=info['Count']
        self.minval=info['MinVal']
        self.stepsize=info['StepSize']
    def calcSummary(self,list):
        histo=[0]*self.count
        floatlist=list.GetFloatList()
        for val in floatlist:
            idx=int(0.01+math.floor((val-self.minval)*1.0/self.stepsize))
            if idx<0:idx=0
            if idx>=self.count: idx=self.count-1
            histo[idx]+=1
        if len(floatlist)>0:
            normhisto=[val*1.0/len(floatlist) for val in histo]
        else:
            normhisto=histo
        return normhisto
    def getInfo(self):
        return {'ID':'Histogram', 'Count':self.count, 'MinVal':self.minval, 'StepSize':self.stepsize }

#############################################################################################


class DataProperty:
    def __init__(self,info):
        self.ID=info['ID']



def ReadJsonFile(filename):
    f = open(filename, 'r')
    body=''
    for line in f.readlines():
        line=line.lstrip()
        if (len(line)>0) and (line[0]!='#'):
            body+=line
    f.close()
    data=simplejson.loads(body)
    return data

#############################################################################################

class DataProvider_TabbedFile:
    def __init__(self,ifilename):
        self.inputfilename=ifilename
    def GetRowIterator(self):
        inputfile=open(self.inputfilename,'r')
        inputcolumns=inputfile.readline().rstrip('\n').split('\t')[1:]
        while True:
            line=inputfile.readline().rstrip('\n')
            if not(line):
                break
            linecomps=line.split('\t')
            rs={}
            rs['position']=int(linecomps[0])
            for colnr in range(len(inputcolumns)):
                rs[inputcolumns[colnr]]=[x for x in linecomps[colnr+1].split(',')]
            yield rs
        inputfile.close()


def CreateDataProvider(filename,filetype):
    if filetype=="TabDelimitedFile":
        return DataProvider_TabbedFile(filename)
    raise Exception('Unknown data type {0}'.format(filetype))



#############################################################################################


class Creator:

    def __init__(self,ibasedir,ifolder,iconfig):
        self.basedir=ibasedir
        self.folder=ifolder
        self.config=iconfig
        self.datadir=self.basedir+'/'+self.folder
        print('Initialising Creator, directory="{0}", config="{1}"'.format(self.datadir,self.config))
        configdata=ReadJsonFile(self.datadir+'/'+self.config+".cnf")

        self.SourceFilePattern=configdata['SourceFilePattern']
        self.SourceFileType=configdata['SourceFileType']
        self.BlockSizeStart=int(configdata['BlockSizeStart'])
        self.BlockSizeIncrFactor=int(configdata['BlockSizeIncrFactor'])
        self.BlockSizeMax=int(configdata['BlockSizeMax'])

        self.properties=[DataProperty(prop) for prop in configdata['Properties']]

        self.summarisers=[]
        self.summarisersIdx={}
        for summinfo in configdata['Summarisers']:
            summ=self.CreateSummariser(summinfo)
            self.summarisersIdx[summ.ID]=len(self.summarisers)
            self.summarisers.append(summ)

        self.encodedRowSize=sum([summ.encoder.getlength() for summ in self.summarisers])

    def CreateSummariser(self,summinfo):
        mysumm=None
        if summinfo['Method']=='Average':
            mysumm=SummariserAverage(summinfo)
        if summinfo['Method']=='Max':
            mysumm=SummariserMax(summinfo)
        if summinfo['Method']=='Min':
            mysumm=SummariserMin(summinfo)
        if summinfo['Method']=='Quantile':
            mysumm=SummariserQuantile(summinfo)
        if summinfo['Method']=='Histogram':
            mysumm=SummariserHistogram(summinfo)
        if summinfo['Method']=='PickRandom':
            mysumm=SummariserPickRandom(summinfo)
        if summinfo['Method']=='MostFrequent':
            mysumm=SummariserMostFrequent(summinfo)
        if mysumm is None:
            raise Exception('Unknown summariser '+summinfo['Method'])
        return mysumm

    def getSummariserNr(self,summid):
        return self.summarisersIdx[summid]


    def GetBlocks(self,blockSize):
        lst=[]
        currentBlockStart=1
        for row in self.dataprovider.GetRowIterator():
            while row['position']>=currentBlockStart+blockSize:
                yield lst
                lst=[]
                currentBlockStart+=blockSize
            lst.append(row)
        yield lst

    def Summarise(self):

        #create output directory if necessary
        outputdir=os.path.join(self.datadir,'Summaries')
        if not os.path.exists(outputdir):
            os.makedirs(outputdir)

        #remove all summary files that correspond to this configuration
        for filename in os.listdir(outputdir):
            if filename.startswith(self.config):
                os.remove(os.path.join(outputdir,filename))


        expr=re.compile(self.SourceFilePattern)
        for sourcefilename in os.listdir(self.datadir):
            fullsourcefilename=os.path.join(self.datadir,sourcefilename)
            if not os.path.isdir(fullsourcefilename):
                if not expr.match(sourcefilename):
                    print('[Skipping file "{0}" (not matched)]'.format(sourcefilename))
                else:
                    print('Processing file "{0}"'.format(sourcefilename))
                    dataid=sourcefilename.split('.')[0]

                    self.dataprovider=CreateDataProvider(fullsourcefilename,self.SourceFileType)
                    outputbasefilename=os.path.join(self.datadir,'Summaries',dataid)
                    blockSize=self.BlockSizeStart
                    while blockSize<=self.BlockSizeMax:
                        print('EXECUTING {0}, BLOCK SIZE {1}'.format(dataid,blockSize))
                        outputfilename=os.path.join(self.datadir,'Summaries','{0}_{1}_{2}'.format(self.config,dataid,str(blockSize)))
                        outputfile=open(outputfilename,'w')

                        blocknr=0
                        for block in self.GetBlocks(blockSize):
                            #for each property, get a list of data in this block
                            proplists={prop.ID: ValueList([val for row in block for val in row[prop.ID]]) for prop in self.properties}
                            #run all the summarisers, merge & write to the output file as a row record
                            summarystr=''.join([summ.Perform(proplists[summ.propID]) for summ in self.summarisers])
                            outputfile.write(summarystr)
                            blocknr+=1
                            if blocknr%5000==0: print('  {0} blocks processed'.format(blocknr))

                        outputfile.close()
                        blockSize *=self.BlockSizeIncrFactor






    def GetData(self,dataid,blockSize,start,length,summarylist):
        outputbasefilename=self.datadir+'/Summaries/'+self.config+'_'+dataid
        filename=outputbasefilename+'_'+str(blockSize)
        #print('Fetching from '+filename)
        f=open(filename,'r')
        f.seek(start*self.encodedRowSize)
        linelength=self.encodedRowSize
        strblock=f.read(length*linelength)
        f.close()
        result={}
        for summid in summarylist:
            nr=self.getSummariserNr(summid)
            strrs=''
            offset=sum([self.summarisers[i].encoder.getlength() for i in range(nr)])
            complength=self.summarisers[nr].encoder.getlength()
            for i in range(length):
                strrs+=strblock[offset:offset+complength]
                offset+=linelength

            result[self.folder+'_'+self.config+'_'+summid]={'data':strrs, 'summariser':self.summarisers[nr].getInfo(), 'encoder':self.summarisers[nr].encoder.getInfo() }
        return result