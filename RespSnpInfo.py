import B64

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

def ReturnSnpInfo(meta,returndata):

#    mytablename=DQXDbTools.ToSafeIdentifier(returndata['tbname'])
    startps=float(returndata['start'])
    endps=float(returndata['stop'])
    seqids=returndata['seqids'].split('~')
    chromoid=returndata['chromoid']
    folder=returndata['folder']

    datadir=meta['BASEDIR']+'/'+folder
    index=GetPositionIndex(datadir,chromoid)
    idx1=index.Pos2IndexLeft(startps)
    idx2=index.Pos2IndexRight(endps)

    coder=B64.ValueListCoder()
    returndata['posits']=coder.EncodeIntegersByDifferenceB64(index.posits[idx1:idx2+1])

    reclen=7
    f=open(datadir+'/'+chromoid+'_snpinfo.txt')
    f.seek(reclen*idx1)
    snpdata=f.read(reclen*(idx2-idx1+1))
    f.close()
    returndata['snpdata']=snpdata

    seqvals={}
    reclen=4
    for seqid in seqids:
        f=open(datadir+'/'+chromoid+'_'+seqid+'.txt')
        f.seek(reclen*idx1)
        seqvals[seqid]=f.read(reclen*(idx2-idx1+1))
        f.close()
    returndata['seqvals']=seqvals

    print('Serving {0} snps, idx={4}-{5} in range {1}-{2} (size {3})'.format(idx2-idx1,startps,endps,endps-startps,idx1,idx2))

    return returndata
