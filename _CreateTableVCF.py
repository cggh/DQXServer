
import math
import random
import B64
import sys
import simplejson
import DQXEncoder
import os
import re
import shlex
import customresponders.uploadtracks.VTTable as VTTable

sourcedir='.'

#"3d7xHb3-qcPlusSamples-01"
#"7g8xGb4-allSamples-01"
#"Hb3xDd2-allSamples-01"
#"svar1"
#"test"

ExpNames = ["3d7_hb3", "7g8_gb4", "hb3_dd2"]
Methods = ['gatk', 'cortex']

ExpName = ExpNames[2]
Method = Methods[1]

#============= FAKE STUFF FOR DEBUGGING; REMOVE FOR PRODUCTION ==============
sys.argv=['',ExpName+'.'+Method+'.final', Method.upper() + 'Crosses_Table']

sourcedir='/home/pvaut/Documents/Genome/SnpDataCross3'
#============= END OF FAKE STUFF ============================================

if len(sys.argv)<2:
    print('Usage: COMMAND VCFFilename [ConfigFilename] [OutputDir]')
    print('   VCFFilename= name of the source VCF file (do not provide the extension ".vcf")')
    print('   ConfigFilename= name of the source configuration file (do not provide the extension ".cnf").')
    print('      If not provided, the same name as the VCF file will be used')
    print('   OutputDir= destination folder of the processed data.')
    print('      If not provided, the same name as the VCF file will be used')
    sys.exit()

dataSource=sys.argv[1]
configSource=dataSource
dataDest=dataSource

if len(sys.argv)>=3:
    configSource=sys.argv[2]

if len(sys.argv)>=4:
    dataDest=sys.argv[3]


print('dataSource='+dataSource)
print('configSource='+configSource)
print('dataDest='+dataDest)


#sys.exit()


class DataProvider_VCF:
    def __init__(self,ifilename,settings):
        self.checkRequiredComponents(settings)
        self.infocomps=settings['InfoComps']
        self.FilterPassedOnly=settings['FilterPassedOnly']
        self.PositiveQualityOnly=settings['PositiveQualityOnly']
        self.inputfilename=ifilename
        inputfile=open(self.inputfilename,'r')
        headerended=False
        self.lineNr=0
        self.headerlen=0
        self.parents=[]
        self.filterList=[]#List if filters
        self.filterMap={}#Map filters to indices
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
                    if (key=='PARENT') or (key=='parents'):
                        self.parents.append(content)
                    if key=='FILTER':
                        items=self.parseHeaderComp(content)
                        self.filterMap[items['ID']]=len(self.filterList)
                        self.filterList.append(items['ID'])

        print('{0} FILTERS: '.format(len(self.filterList))+','.join(self.filterList))


        #parse header line
        line=line[1:]
        self.headerLine = line
        headercomps=line.split('\t')
        self.colnr_chrom=-1
        self.colnr_pos=-1
        self.colnr_qual=-1
        self.colnr_filter=-1
        self.colnr_format=-1
        self.colnr_info=-1
        headercompdict={}
        for compnr in range(len(headercomps)):
            headercompdict[headercomps[compnr]]=compnr
            if headercomps[compnr]=='CHROM': self.colnr_chrom=compnr
            if headercomps[compnr]=='POS': self.colnr_pos=compnr
            if headercomps[compnr]=='QUAL': self.colnr_qual=compnr
            if headercomps[compnr]=='FILTER': self.colnr_filter=compnr
            if headercomps[compnr]=='FORMAT': self.colnr_format=compnr
            if headercomps[compnr]=='INFO': self.colnr_info=compnr

        if self.colnr_chrom<0: raise Exception('Field CHROM not found')
        if self.colnr_pos<0: raise Exception('Field CHROM not found')
        if self.colnr_qual<0: raise Exception('Field QUAL not found')
        if self.colnr_filter<0: raise Exception('Field FILTER not found')
        if self.colnr_format<0: raise Exception('Field FORMAT not found')

        self.sampleids=headercomps[self.colnr_format+1:]
        self.sampleids=[x.replace('/','__') for x in self.sampleids]

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

    def parseHeaderComp(self,content):
        if (content[0]!='<') and (content[len(content)-1]!='>'):
            raise Exception('Invalid header line: '+content)
        content=content[1:-1]
        my_splitter = shlex.shlex(content, posix=True)
        my_splitter.whitespace += ','
        my_splitter.whitespace_split = True
        result={}
        for token in my_splitter:
            subtokens=token.split('=',1)
            key=subtokens[0]
            value=subtokens[1]
            result[key]=value
        return result

    def checkRequiredComponents(self,settings):

        for requiredTag in ['SourceFileFormat','FilterPassedOnly','PositiveQualityOnly','InfoComps']:
            if requiredTag not in settings:
                raise Exception('Tag "{0}" not present in settings file'.format(requiredTag))

        #Check presence of required tags for the per-position SNP component
        for requiredTag in ['ID','Source', 'Type']:
            for comp in settings['InfoComps']:
                if requiredTag not in comp:
                    raise Exception('Tag "{0}" not present in SampleComponent {1}'.format(requiredTag,str(comp)))


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

                    #parse filter data
                    filterFlags=[False]*len(self.filterList)
                    if linecomps[self.colnr_filter]!='PASS':
                        for filterItem in linecomps[self.colnr_filter].split(';'):
                            if filterItem not in self.filterMap:
                                raise Exception('Invalid filter item: '+filterItem)
                            filterFlags[self.filterMap[filterItem]]=True
                    rs['filter']=filterFlags

                    #parse info field data
                    infodict={}
                    for comp in linecomps[self.colnr_info].split(';'):
                        if '=' in comp:
                            key,val=comp.split('=')
                            infodict[key]=val.split(',')
                            if key == 'EFF':
                                part1, part2 = val.split(',')[0][:-2].split('(')
                                infodict[key] = part2.split('|')
                                infodict[key].insert(0, part1)
                                pass
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
                                vl = None
                                print('Missing info component {0}:{1} for line {2}.\nDATA: {3}'.format(infocomp['fieldKey'],infocomp['keyIndex'],self.lineNr,str(infodict)))
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

                        yield rs

        inputfile.close()









#Load settings
settingsFile=open('{0}/{1}.cnf'.format(sourcedir,configSource))
settingsStr=''
for line in settingsFile:
    if (len(line)>0) and (line[0]!='#'):
        settingsStr+=line
settingsFile.close()
settings=simplejson.loads(settingsStr)
sourceFileName='{0}/{1}.vcf'.format(sourcedir,dataSource)


sourceFile=DataProvider_VCF(sourceFileName,settings)

headerFileName='{0}/{1}.header.txt'.format(sourcedir,dataSource)
hfp = open(headerFileName, 'w')
hfp.write(sourceFile.headerLine+'\n')
hfp.close()


print('=============== Report Snp Position Information components ============')
for infocomp in settings['InfoComps']:
    print("ID={0}".format(infocomp['ID']))
print('=======================================================================')


print('SAMPLES: '+','.join(sourceFile.sampleids))

################# Create metainfo file #########################################
ofilename = '{0}/table.txt'.format(sourcedir)
ofile=open(ofilename,'w')
infocompinfo=[]
#Filter flag booleanlist
infocompinfo.append({'ID':'FilterFlags', 'Name':'FilterFlags', 'Display':False, 'DataType':'BooleanList', "Encoder": {"ID": "BooleanListB64", "Count": len(sourceFile.filterList)}})

#Other properties
for infocomp in settings['InfoComps']:
    infoinfo={'ID': infocomp['ID']}
    if 'Min' in infocomp: infoinfo['Min']=infocomp['Min']
    if 'Max' in infocomp: infoinfo['Max']=infocomp['Max']
    infocompinfo.append(infoinfo)


ofile.write('ExpName\tMethod\t')
ofile.write('\t'.join([comp['ID'] for comp in settings['InfoComps']]))
ofile.write('\t')
ofile.write('\t'.join(sourceFile.filterList))
ofile.write('\n')

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

    if ('ConvertChromoNamesV32Pf3D7' in settings) and (settings['ConvertChromoNamesV32Pf3D7']):
        chromname=chromname.replace('_v3', '')



#Write SNP info components
    line = []
    for infocomp in settings['InfoComps']:
        vl=rw[infocomp['ID']]
        if vl is None:
            vl= ''
        if 'MaxLen' in infocomp:
            maxlen = infocomp['MaxLen']
            if len(vl) > maxlen:
                vl = vl[:maxlen]+'...'
        if 'Default' in infocomp:
            if vl == '':
                vl = infocomp['Default']
        line.append(str(vl))
    ofile.write(ExpName+'\t')
    ofile.write(Method+'\t')
    ofile.write('\t'.join(line))
    ofile.write('\t')
    ofile.write('\t'.join([str(int(state)) for state in rw['filter']]))
    ofile.write('\n')




    nr+=1
    if nr%500==0:
        print('Processed: '+str(nr))
    if (limitcount is not None) and (nr>=limitcount):
        print('>>> Truncated data processing at {0}'.format(limitcount))
        break

print('============= Completed! =========================')

ofile.close()


tb = VTTable.VTTable()
tb.allColumnsText = True
tb.LoadFile(ofilename)
for comp in settings['InfoComps']:
    if (comp['Type'] == 'Float') or (comp['Type'] == 'Int'):
        tb.ConvertColToValue(comp['ID'])
for filter in sourceFile.filterList:
    tb.ConvertColToValue(filter)
tb.PrintRows(0,20)

ofilename = "{0}_{1}".format(ExpName, Method)
tb.SaveSQLCreation('{0}/{1}_create.sql'.format(sourcedir, ofilename), 'variants3')
tb.SaveSQLDump('{0}/{1}_dump.sql'.format(sourcedir, ofilename), 'variants3')