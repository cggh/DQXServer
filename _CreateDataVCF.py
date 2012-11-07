import math
import random
import B64
import sys


if False:
    f=open('C:/Data/Genomes/PfCrosses/InputData/Snps/3d7xHb3-qcPlusSamples-0.1.vcf','r')
    st=''
    for i in range(1000):
        st+=f.readline()
    f.close()
    f=open('C:/Data/Genomes/top.txt','w')
    f.write(st)
    f.close()
    sys.exit()

class DataProvider_VCF:
    def __init__(self,ifilename,settings):

        self.infocomps=settings['infocomps']
        self.samplecomps=settings['samplecomps']

        self.inputfilename=ifilename
        inputfile=open(self.inputfilename,'r')
        headerended=False
        self.headerlen=0
        while not(headerended):
            self.headerlen+=1
            line=inputfile.readline().rstrip('\n')
            if (line[0]=='#') and (line[1]!='#'):
                headerended=True

        #parse header line
        line=line[1:]
        headercomps=line.split('\t')
        self.colnr_chrom=-1
        self.colnr_pos=-1
        self.colnr_qual=-1
        self.colnr_ref=-1
        self.colnr_alt=-1
        self.colnr_format=-1
        self.colnr_info=-1
        self.colnr_filter=-1
        for compnr in range(len(headercomps)):
            if headercomps[compnr]=='CHROM': self.colnr_chrom=compnr
            if headercomps[compnr]=='POS': self.colnr_pos=compnr
            if headercomps[compnr]=='QUAL': self.colnr_qual=compnr
            if headercomps[compnr]=='REF': self.colnr_ref=compnr
            if headercomps[compnr]=='ALT': self.colnr_alt=compnr
            if headercomps[compnr]=='FORMAT': self.colnr_format=compnr
            if headercomps[compnr]=='INFO': self.colnr_info=compnr
            if headercomps[compnr]=='FILTER': self.colnr_filter=compnr

        if self.colnr_chrom<0: raise Exception('Field CHROM not found')
        if self.colnr_pos<0: raise Exception('Field CHROM not found')
        if self.colnr_qual<0: raise Exception('Field QUAL not found')
        if self.colnr_format<0: raise Exception('Field FORMAT not found')
        if self.colnr_filter<0: raise Exception('Field FILTER not found')

        self.sampleids=headercomps[self.colnr_format+1:]

        inputfile.close()

    def GetRowIterator(self):
        inputfile=open(self.inputfilename,'r')
        for i in range(self.headerlen):
            inputfile.readline()

        while True:
            line=inputfile.readline().rstrip('\n')
            if not(line):
                break
            linecomps=line.split('\t')
            rs={}
            rs['chrom']=linecomps[self.colnr_chrom]
            rs['pos']=int(linecomps[self.colnr_pos])
            rs['ref']=linecomps[self.colnr_ref]
            rs['alt']=linecomps[self.colnr_alt]
            if linecomps[self.colnr_qual]=='.':
                rs['qual']=1
            else:
                rs['qual']=float(linecomps[self.colnr_qual])
            rs['filter']=linecomps[self.colnr_filter]=='PASS'
            if rs['qual']>0:

                #parse info field data
                infodict={}
                for comp in linecomps[self.colnr_info].split(';'):
                    if '=' in comp:
                        key,val=comp.split('=')
                        infodict[key]=val.split(',')
                    else:
                        infodict[comp]=[1]

                for infocomp in self.infocomps:
                    vl=0
                    if infocomp['id'] in infodict:
                        vl=infodict[infocomp['id']][infocomp['sub']]
                    if 'cats' in infocomp:
                        vl=infocomp['cats'][vl]
                        if vl is None:
                            raise Exception('Unknown coding ')
                    rs[infocomp['name']]=vl


                #parse format
                formatcomps=linecomps[self.colnr_format].split(':')
                samplecompposits=[]
                for samplecompnr in range(len(self.samplecomps)):
                    thesamplecomppos=-1
                    for fcompnr in range(len(formatcomps)):
                        if self.samplecomps[samplecompnr]['id']==formatcomps[fcompnr]:
                            thesamplecomppos=fcompnr
                    #if thesamplecomppos<0: raise Exception('unable to find format component {0} in line {1}'.format(self.samplecomps[samplecompnr]['id'],line))
                    samplecompposits.append(thesamplecomppos)

                #parse sample data
                scolnr=self.colnr_format
                for sid in self.sampleids:
                    scolnr+=1
                    samplecompvals=[x.split(',') for x in linecomps[scolnr].split(':')]
                    scompnr=0
                    for scomp in self.samplecomps:
                        theval=0
                        if samplecompposits[scompnr]>=0:
                            theval=samplecompvals[samplecompposits[scompnr]][scomp['sub']]
                        rs[sid+'_'+scomp['name']]=theval
                        scompnr+=1

                yield rs
        inputfile.close()









def CodeFloat(vl,minvl,maxvl,bytecount):
    CompressedRange=int(64**bytecount-10)
    ivl=int(0.5+(float(vl)-minvl)/(maxvl-minvl)*CompressedRange)
    if ivl<0: ivl=0
    if ivl>CompressedRange: ivl=CompressedRange
    return b64.Int2B64(ivl,bytecount)


def CodeBoolean(vl):
    if vl:
        return '1'
    else:
        return '0'


lastchr='---'
files={}

def GetWriteFile(chrom,id):
    global lastchr
    if lastchr!=chrom:
        for fname in files:
#            print(fname)
            if fname.startswith(lastchr):
#                print('* Closing {0}'.format(fname))
                files[fname].close()
        lastchr=chrom
    fid=chrom+'_'+id
    if not(fid in files):
        files[fid]=open('C:/Data/Test/Genome/{0}/{1}.txt'.format(dataid,fid),'w')
    return files[fid]





#fileid='3d7xHb3-qcPlusSamples-0.1'
#fileid='7g8xGb4-allSamples-0.1'
#fileid='Hb3xDd2-allSamples-0.1'

if False:
    settings={
        'fileid':'Hb3xDd2-allSamples-0.1',
        'infocomps':
            [
                    {'name':'AQ','id':'AQ','sub':0},
                    {'name':'MQ','id':'MQ','sub':0}
                #            {'name':'ST','id':'ST','sub':0, 'cats':{'intergenic':0,'intron':1,'coding':2,'unknown':99}}
            ],
        'samplecomps':
            [
                    {'name':'covA', 'id':'AD','sub':0},
                    {'name':'covD','id':'AD','sub':1}
            ]
    }


if True:
    settings={
        'fileid':'svar1',
        'passonly':True,
        'infocomps':
               [
                {'name':'AQ','id':'SVLEN','sub':0},
                {'name':'MQ','id':'SVLEN','sub':0},
                {'name':'SVTYPE','id':'SVTYPE','sub':0, 'cats':{'SNP':0,'SNP_FROM_COMPLEX':0,'INDEL':1,'INV_INDEL':1,'INDEL_FROM_COMPLEX':1,'INS':2,'DEL':3}}
               ],
        'samplecomps':
        [
                {'name':'covA', 'id':'COV','sub':0},
                {'name':'covD','id':'COV','sub':1}
        ]
    }




dataid='SNP-'+settings['fileid'].replace('.','')
filename='C:/Data/Genomes/PfCrosses/InputData/Snps/{0}.vcf'.format(settings['fileid'])

f=open(filename,'r')
st=''
for i in range(1000):
    st+=f.readline()
f.close()
f=open('C:/Data/Genomes/PfCrosses/InputData/Snps/TOP_{0}.txt'.format(settings['fileid']),'w')
f.write(st)
f.close()


fl=DataProvider_VCF(filename,settings)

b64=B64.B64()
nr=0
for rw in fl.GetRowIterator():
    if (rw['filter']) or not(settings['passonly']):
        chromname=rw['chrom']
        if chromname[:3]=='MAL':
            chromnr=int(chromname[3:])
            chromname=str(chromnr).zfill(2)
            chromname='Pf3D7_'+chromname

        GetWriteFile(chromname,'pos').write('{0}\n'.format(rw['pos']))

        of=GetWriteFile(chromname,'snpinfo')

        written=False
        if rw['SVTYPE']==0:
            of.write(rw['ref'])
            of.write(rw['alt'])
            written=True
        if rw['SVTYPE']==1:
            of.write('+')
            of.write('+')
            written=True
        if rw['SVTYPE']==2:
            of.write('.')
            of.write('+')
            written=True
        if rw['SVTYPE']==3:
            of.write('+')
            of.write('.')
            written=True

        if not(written): raise Exception('Invalid SVTYPE')

        of.write(CodeFloat(rw['AQ'],0,100,2))
        of.write(CodeFloat(rw['MQ'],0,100,2))
        of.write(CodeBoolean(rw['filter']))

        for sid in fl.sampleids:
            of=GetWriteFile(chromname,sid)
            of.write(b64.Int2B64(int(rw[sid+'_covA']),2))
            of.write(b64.Int2B64(int(rw[sid+'_covD']),2))
            #print(rw)

    nr+=1
    if nr%1000==0:
        print('Processed: '+str(nr))
#    if nr==30000:
#        break

