import math
import random
import B64
import sys
import simplejson
import DQXEncoder
import os

sourcedir='C:/Data/Test/Genome/SnpDataCross'
#dataid="3d7xHb3-qcPlusSamples-01"
#dataid="7g8xGb4-allSamples-01"
#dataid="Hb3xDd2-allSamples-01"
#dataid="svar1"
#dataid="test"

dataid="PG0233-C"

if len(sys.argv)==2:
    dataid=sys.argv[1]
    sourcedir='.'


print('dataid='+str(dataid))




class DataProvider_VCF:
    def __init__(self,ifilename,settings):
        self.checkRequiredComponents(settings)
        self.infocomps=settings['InfoComps']
        self.samplecomps=settings['SampleComps']
        self.FilterPassedOnly=settings['FilterPassedOnly']
        self.PositiveQualityOnly=settings['PositiveQualityOnly']
        self.inputfilename=ifilename
        inputfile=open(self.inputfilename,'r')
        headerended=False
        self.lineNr=0
        self.headerlen=0
        self.parents=[]
        while not(headerended):
            self.headerlen+=1
            line=inputfile.readline().rstrip('\n')
            self.lineNr+=1
            if (line[0]=='#') and (line[1]!='#'):
                headerended=True
            else:
                tokens=line[2:].split('=',1)
                if len(tokens)==2:
                    key=tokens[0]
                    content=tokens[1]
                    if key=='PARENT':
                        self.parents.append(content)

        #parse header line
        line=line[1:]
        headercomps=line.split('\t')
        self.colnr_chrom=-1
        self.colnr_pos=-1
        self.colnr_qual=-1
        self.colnr_format=-1
        self.colnr_info=-1
        headercompdict={}
        for compnr in range(len(headercomps)):
            headercompdict[headercomps[compnr]]=compnr
            if headercomps[compnr]=='CHROM': self.colnr_chrom=compnr
            if headercomps[compnr]=='POS': self.colnr_pos=compnr
            if headercomps[compnr]=='QUAL': self.colnr_qual=compnr
            if headercomps[compnr]=='FORMAT': self.colnr_format=compnr
            if headercomps[compnr]=='INFO': self.colnr_info=compnr

        if self.colnr_chrom<0: raise Exception('Field CHROM not found')
        if self.colnr_pos<0: raise Exception('Field CHROM not found')
        if self.colnr_qual<0: raise Exception('Field QUAL not found')
        if self.colnr_format<0: raise Exception('Field FORMAT not found')

        self.sampleids=headercomps[self.colnr_format+1:]

        inputfile.close()

        ## Map Snp info components to columns in the file
        for infocomp in self.infocomps:
            sourcecomps=infocomp['Source'].split(':')
            if sourcecomps[0]=='INFO':
                infocomp['inInfo']=True
                infocomp['colNr']=self.colnr_info
                infocomp['fieldKey']=sourcecomps[1]
                infocomp['keyIndex']=int(sourcecomps[2])
                pass
            else:
                infocomp['inInfo']=False
                if len(infocomp['Source'])>0:
                    try:
                        infocomp['colNr']=headercompdict[infocomp['Source']]
                    except KeyError:
                        raise Exception('Missing Column "{0}"'.format(infocomp['Source']))
                pass

    def checkRequiredComponents(self,settings):

        for requiredTag in ['SourceFile','SourceFileFormat','FilterPassedOnly','PositiveQualityOnly','InfoComps','SampleComps']:
            if requiredTag not in settings:
                raise Exception('Tag "{0}" not present in settings file'.format(requiredTag))

        #Check presence of required tags for the per-position SNP component
        for requiredTag in ['Name','ID','Source', 'Display', 'Encoder']:
            for comp in settings['InfoComps']:
                if requiredTag not in comp:
                    raise Exception('Tag "{0}" not present in SampleComponent {1}'.format(requiredTag,str(comp)))

        #Check presence of required per-position SNP components
        requiredComponents = ['RefBase', 'AltBase', 'Filtered']
        for requiredCompID in requiredComponents:
            found=False
            for comp in settings['InfoComps']:
                if comp['ID']==requiredCompID:
                    found=True
            if not(found):
                raise Exception('Missing require per-sample component "{0}"'.format(requiredCompID))

        #Check presence of required tags for each per-sample SNP component
        for requiredTag in ['ID','SourceID','SourceSub']:
            for comp in settings['SampleComps']:
                if requiredTag not in comp:
                    raise Exception('Tag "{0}" not present in InfoComponent {1}'.format(requiredTag,str(comp)))

        #Check presence of required per-sample SNP components
        requiredComponents = ['covA', 'covD']
        for requiredCompID in requiredComponents:
            found=False
            for comp in settings['SampleComps']:
                if comp['ID']==requiredCompID:
                    found=True
            if not(found):
                raise Exception('Missing required per-sample component "{0}"'.format(requiredCompID))
        #Currently, the set of components that should be present here is exactly defined
        if len(settings['SampleComps'])>len(requiredComponents):
            raise Exception('Too many per-sample components')
        pass


    def GetRowIterator(self):
        inputfile=open(self.inputfilename,'r')
        for i in range(self.headerlen):
            inputfile.readline()

        while True:
            line=inputfile.readline().rstrip('\n')
            if not(line):
                break
            self.lineNr+=1
            linecomps=line.split('\t')
            if len(linecomps)>1:
                rs={}
                rs['chrom']=linecomps[self.colnr_chrom]
                rs['pos']=int(linecomps[self.colnr_pos])
                if linecomps[self.colnr_qual]=='.':
                    rs['qual']=1
                else:
                    rs['qual']=float(linecomps[self.colnr_qual])
                accept=True
                if (self.PositiveQualityOnly) and (not(rs['qual']>0)): accept=False
                if accept:

                    #parse info field data
                    infodict={}
                    for comp in linecomps[self.colnr_info].split(';'):
                        if '=' in comp:
                            key,val=comp.split('=')
                            infodict[key]=val.split(',')
                        else:
                            infodict[comp]=[1]

                    #Get all the Snp Position info fields
                    for infocomp in self.infocomps:
                        vl=None
                        if not(infocomp['inInfo']):
                            if 'colNr' in infocomp:
                                try:
                                    vl=linecomps[infocomp['colNr']]
                                except (IndexError):
                                    raise Exception('Missing component {0} for line {1}.\nDATA: {2}'.format(infocomp['colNr'],self.lineNr,str(linecomps)))
                        else:
                            try:
                                vl=infodict[infocomp['fieldKey']][infocomp['keyIndex']]
                            except (KeyError,IndexError):
                                raise Exception('Missing info component {0}:{1} for line {2}.\nDATA: {3}'.format(infocomp['fieldKey'],infocomp['keyIndex'],self.lineNr,str(infodict)))
                        if 'Categories' in infocomp:
                            if vl in infocomp['Categories']:
                                vl=infocomp['Categories'][vl]
                            else:
                                if '*' in infocomp['Categories']:
                                    vl=infocomp['Categories']['*']
                                else:
                                    raise Exception('Unknown coding ')
                        rs[infocomp['ID']]=vl

                    if (self.FilterPassedOnly) and not(rs['Filtered']): accept=False
                    if accept:

                        #parse format identifier
                        formatcomps=linecomps[self.colnr_format].split(':')
                        samplecompposits=[]
                        for samplecompnr in range(len(self.samplecomps)):
                            thesamplecomppos=-1
                            for fcompnr in range(len(formatcomps)):
                                if self.samplecomps[samplecompnr]['SourceID']==formatcomps[fcompnr]:
                                    thesamplecomppos=fcompnr
                            if thesamplecomppos<0:
                                raise Exception('Unable to find format component "{0}" in line {1}\nFORMAT: {2}'.format(self.samplecomps[samplecompnr]['SourceID'],self.lineNr,linecomps[self.colnr_format]))
                            samplecompposits.append(thesamplecomppos)

                        #parse per-sample data
                        scolnr=self.colnr_format
                        for sid in self.sampleids:
                            scolnr+=1
                            samplecompvals=[x.split(',') for x in linecomps[scolnr].split(':')]
                            scompnr=0
                            for scomp in self.samplecomps:
                                theval=0
                                if samplecompposits[scompnr]>=0:
                                    try:
                                        theval=samplecompvals[samplecompposits[scompnr]][scomp['SourceSub']]
                                    except (KeyError,IndexError):
                                        raise Exception('Unable to get per-sample information component "{0}" in line {1}\nFORMAT: {2}\nDATA: {3}'.format(scomp['ID'],self.lineNr,linecomps[self.colnr_format],linecomps[scolnr]))
                                rs[sid+'_'+scomp['ID']]=theval
                                scompnr+=1

                        yield rs

        inputfile.close()








lastchr='---'
files={}

def GetWriteFile(chrom,id):
    global lastchr
    if lastchr!=chrom:
        for fname in files:
            if fname.startswith(lastchr):
                files[fname].close()
        lastchr=chrom
    fid=chrom+'_'+id
    if not(fid in files):
        files[fid]=open('{0}/{1}/{2}.txt'.format(sourcedir,dataid,fid),'w')
    return files[fid]



#Create output directory
outputdir=sourcedir+'/'+dataid
if not os.path.exists(outputdir):
    os.makedirs(outputdir)
#remove all output files that correspond to this configuration
for flename in os.listdir(outputdir):
    os.remove(os.path.join(outputdir,flename))


#Load settings
settingsFile=open('{0}/{1}.txt'.format(sourcedir,dataid))
settingsStr=''
for line in settingsFile:
    if (len(line)>0) and (line[0]!='#'):
        settingsStr+=line
settingsFile.close()
settings=simplejson.loads(settingsStr)
sourceFileName='{0}/{1}.vcf'.format(sourcedir,settings['SourceFile'])

#For reference: write top lines of the VCF file to the output directory
f=open(sourceFileName,'r')
st=''
for i in range(3000):
    st+=f.readline()
f.close()
f=open('{0}/_TOP_VCF_{1}.txt'.format(outputdir,dataid),'w')
f.write(st)
f.close()


sourceFile=DataProvider_VCF(sourceFileName,settings)



print('=============== Report Snp Position Information components ============')
for infocomp in settings['InfoComps']:
    print("ID={0}".format(infocomp['ID']))
    print("    Name={0}".format(infocomp['Name']))
    infocomp['theEncoder']=DQXEncoder.GetEncoder(infocomp['Encoder'])
    print("    Encoder={0}".format(str(infocomp['theEncoder'].getInfo())))
print('=======================================================================')

print('SAMPLES: '+','.join(sourceFile.sampleids))

################# Create metainfo file #########################################
ofile=open('{0}/_MetaData.txt'.format(outputdir,dataid),'w')
ofile.write('Samples='+'\t'.join(sourceFile.sampleids)+'\n')
infocompinfo=[]
for infocomp in settings['InfoComps']:
    infoinfo={'ID': infocomp['ID'], 'Name': infocomp['Name'], 'Display': infocomp['Display']}
    if 'Min' in infocomp: infoinfo['Min']=infocomp['Min']
    if 'Max' in infocomp: infoinfo['Max']=infocomp['Max']
    infoinfo['Encoder']=infocomp['theEncoder'].getInfo()
    infoinfo['DataType']=infocomp['theEncoder'].getDataType()
    infocompinfo.append(infoinfo)
ofile.write('SnpPositionFields='+simplejson.dumps(infocompinfo)+'\n')
if len(sourceFile.parents)>0:
    ofile.write('Parents='+'\t'.join(sourceFile.parents)+'\n')
ofile.close()

limitcount=None
if ('LimitCount' in settings):
    limitcount=settings['LimitCount']
    if limitcount<0: limitcount=None



b64=B64.B64()
nr=0
for rw in sourceFile.GetRowIterator():

    chromname=rw['chrom']

    if ('ConvertChromoNamesMAL2Pf3D7' in settings) and (settings['ConvertChromoNamesMAL2Pf3D7']):
        if chromname[:3]=='MAL':
            chromnr=int(chromname[3:])
            chromname=str(chromnr).zfill(2)
            chromname='Pf3D7_'+chromname

    GetWriteFile(chromname,'pos').write('{0}\n'.format(rw['pos']))

    if 'SVTYPE' in rw:
        if rw['SVTYPE']==1:
            rw['RefBase']='+'
            rw['AltBase']='+'
        if rw['SVTYPE']==2:
            rw['RefBase']='.'
            rw['AltBase']='+'
        if rw['SVTYPE']==3:
            rw['RefBase']='+'
            rw['AltBase']='.'

    if len(rw['RefBase'])>1: rw['RefBase']='+';
    if len(rw['AltBase'])>1: rw['AltBase']='+';

    of=GetWriteFile(chromname,'snpinfo')
    for infocomp in settings['InfoComps']:
        vl=rw[infocomp['ID']]
        st=infocomp['theEncoder'].perform(vl)
        if len(st)!=infocomp['theEncoder'].getlength():
            raise Exception('Invalid encoded length')
        of.write(st)

    for sid in sourceFile.sampleids:
        of=GetWriteFile(chromname,sid)
        st=b64.Int2B64(int(rw[sid+'_covA']),2)+b64.Int2B64(int(rw[sid+'_covD']),2)
        of.write(st)

    nr+=1
    if nr%1000==0:
        print('Processed: '+str(nr))
    if (limitcount is not None) and (nr>=limitcount):
        print('>>> Truncated data processing at {0}'.format(limitcount))
        break

print('============= Completed! =========================')