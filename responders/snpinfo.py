import B64
import config

class PositionIndex:
    def __init__(self,datadir,ichromoid):
        self.chromoid=ichromoid
        print('Loading position index {0} {1}'.format(datadir,self.chromoid))
        f=open(datadir+'/'+self.chromoid+'_pos.txt')
        self.posits=[int(x) for x in f.readlines()]
        #check that they are in order
        for i in range(len(self.posits)-1):
            if self.posits[i+1]<self.posits[i]:
                raise Exception('Positions are not sorded')
        f.close()
    def Pos2IndexLeft(self,pos):
        i1 = 0
        p1 = self.posits[i1]
        i2 = len(self.posits)-1
        p2 = self.posits[i2]
        while (p1 < pos) and (i2 > i1 + 1):
            i3 = int((i1 + i2) / 2.0)
            p3=self.posits[i3]
            if p3 >= pos:
                i2 = i3
                p2 = p3
            else:
                i1 = i3
                p1 = p3
        return i1
    def Pos2IndexRight(self,pos):
        i1 = 0
        p1 = self.posits[i1]
        i2 = len(self.posits)-1
        p2 = self.posits[i2]
        while (p2 > pos) and (i2 > i1 + 1):
            i3 = int((i1 + i2) / 2.0)
            p3=self.posits[i3]
            if p3 <= pos:
                i1 = i3
                p1 = p3
            else:
                i2 = i3
                p2 = p3
        return i2

indexes={}

def GetPositionIndex(datadir,chromoid):
    global indexes
    id=datadir+'_'+chromoid
    if not(id in indexes):
        indexes[id]=PositionIndex(datadir,chromoid)
    return indexes[id]

def response(returndata):
#    mytablename=DQXDbTools.ToSafeIdentifier(returndata['tbname'])
    startps=float(returndata['start'])
    endps=float(returndata['stop'])
    seqids=returndata['seqids'].split('~')
    chromoid=returndata['chromoid']
    folder=returndata['folder']
    filterStatus= [a=='1' for a in list(returndata['filters'])]
    snpInfoRecLen=int(returndata['snpinforeclen'])

    filterCount=len(filterStatus)

    datadir=config.BASEDIR+'/'+folder
    index=GetPositionIndex(datadir,chromoid)
    idx1=index.Pos2IndexLeft(startps)
    idx2=index.Pos2IndexRight(endps)
    origSNPCount = idx2-idx1+1

    coder=B64.ValueListCoder()
    b64=B64.B64()
    origPosits=index.posits[idx1:idx2+1]

    f=open(datadir+'/'+chromoid+'_snpinfo.txt')
    f.seek(snpInfoRecLen*idx1)
    snpdata=f.read(snpInfoRecLen*origSNPCount)
    f.close()

    seqvals={}
    snpCallRecLen=4
    for seqid in seqids:
        f=open(datadir+'/'+chromoid+'_'+seqid+'.txt')
        f.seek(snpCallRecLen*idx1)
        seqvals[seqid]=f.read(snpCallRecLen*origSNPCount)
        f.close()

    origPassedList=[]
    for i in range(origSNPCount):
        filterValues = b64.B642BooleanList(snpdata,i*snpInfoRecLen,filterCount)
        if len(filterValues)!=filterCount:
            raise Exception('???')
        passed=True
        for filterTestNr in range(filterCount):
            if (filterStatus[filterTestNr]) and (filterValues[filterTestNr]):
                passed=False
        origPassedList.append(passed)

    passedPosits=[]
    passedSNPData=[]
    passedSeqVals={ seqid:[] for seqid in seqids}
    for i in range(origSNPCount):
        if origPassedList[i]:
            passedPosits.append(origPosits[i])
            passedSNPData.append(snpdata[i*snpInfoRecLen:(i+1)*snpInfoRecLen])
            for seqid in seqids:
                passedSeqVals[seqid].append(seqvals[seqid][i*snpCallRecLen:(i+1)*snpCallRecLen])

    returndata['posits']=coder.EncodeIntegersByDifferenceB64(passedPosits)
    returndata['snpdata']=''.join(passedSNPData)
    returndata['seqvals']={seqid:(''.join(passedSeqVals[seqid])) for seqid in passedSeqVals}


    print('Serving {0} snps, idx={4}-{5} in range {1}-{2} (size {3})'.format(len(passedPosits),startps,endps,endps-startps,idx1,idx2))

    return returndata
