from DQXTableUtils import VTTable
import sys
import urllib


def appendfeatproperty(feat, prop, value):
    value = urllib.unquote_plus(value)
    if len(value) > 0:
        if len(feat[prop]) > 0:
            feat[prop] += ';'
        feat[prop] += value


class GFFParser:
    def __init__(self):
        self.maxrowcount = -1
        self.targetfeaturelist = ['gene', 'pseudogene']
        self.features = []
        self.exonid = 'exon'
        self.attriblist_name = []
        self.attriblist_names = []
        self.attriblist_descr = []


    def GetParentFeature(self,feat):
        parentid=feat['parentid']
        if len(parentid)==0:
            return None
        key=feat['seqid']+parentid
        if not(key in self.featindex):
            return None
        idx=self.featindex[key]
        #print(idx)
        return self.features[idx]


    def printSettings(self):
        print('Max count: '+str(self.maxrowcount))
        print('Gene Ids: '+str(self.targetfeaturelist))
        print('Exon Id: '+self.exonid)
        print('Name attributes: ' + ','.join(self.attriblist_name))
        print('Names attributes: ' + ','.join(self.attriblist_names))
        print('Description attributes: ' + ','.join(self.attriblist_descr))


    def parseGTF(self,filelist):
        print('PARSING GTF')
        self.printSettings()
        #read the feature list
        self.features=[]
        self.featindex={}
        ftnr = 0
        for filename in filelist:
            print('processing file '+filename)
            f=open(filename,'r')
            filerownr = 0
            for line in f.readlines():
                if (self.maxrowcount > 0) and (filerownr > self.maxrowcount):
                    break
                filerownr += 1
                line=line.rstrip('\n')
                if line[0]!='#':
                    parts=line.split('\t')
                    feattype=parts[2]
                    if (feattype in self.targetfeaturelist) or (feattype==self.exonid):
                        if len(self.features)%1000==0:
                            print('read: '+str(len(self.features)))
                        feat={}
                        feat['nr']=len(self.features)
                        feat['children']=[]
                        feat['seqid']=parts[0]
                        feat['type']=feattype
                        feat['start']=int(parts[3])
                        feat['end']=int(parts[4])
                        attribs=parts[8].split(';')
                        feat['id']=str(ftnr)
                        ftnr += 1
                        feat['parentid']=''
                        feat['name']=''
                        feat['names']=''
                        feat['descr']=''
                        for attribstr in attribs:
                            attribstr=attribstr.lstrip()
                            attribstr=attribstr.rstrip()
                            key,sp,value=attribstr.partition(' "')
                            value = value[:-1]
                            if feattype in self.targetfeaturelist:
                                if key == 'gene_id':
                                    feat['id'] = value
                                if key in self.attriblist_name:
                                    appendfeatproperty(feat, 'name', value)
                                if key in self.attriblist_names:
                                    appendfeatproperty(feat, 'names', value)
                                if key in self.attriblist_descr:
                                    appendfeatproperty(feat, 'descr', value)
                            else:
                                if key == 'gene_id':
                                    feat['parentid'] = value
                        self.features.append(feat)
            f.close()


    def parseGFF(self,filelist):
        print('PARSING GFF')
        self.printSettings()
        #read the feature list
        self.features=[]
        tokenMap = {}
        for filename in filelist:
            print('processing file '+filename)
            annotationreading=False
            f=open(filename,'r')
            filerownr = 0
            for line in f.readlines():
                if (self.maxrowcount > 0) and (filerownr > self.maxrowcount):
                    break
                if annotationreading:
                    filerownr += 1
                line=line.rstrip('\n')
                if line=='##gff-version 3' or line=='##gff-version\t3':
                    annotationreading=True
                if line=='##FASTA':
                    annotationreading=False
                if line[0]=='#':
                    print(line)
                if (line[0]!='#') and (annotationreading):
                    parts=line.split('\t')
                    feat={}
                    feat['children']=[]
                    feat['seqid']=parts[0]
                    feat['type']=parts[2]
                    feat['start']=int(parts[3])
                    feat['end']=int(parts[4])
                    attribs=parts[8].split(';')
                    feat['id']=''
                    feat['parentid']=''
                    feat['name'] = ''
                    feat['names'] = ''
                    feat['descr'] = feat['type']
                    for attribstr in attribs:
                        if '=' in attribstr:
                            key, value = attribstr.split('=')
                            tokenMap[key] = ''
                            if key == 'ID':
                                feat['id'] = value
                            if key == 'Parent':
                                feat['parentid'] = value

                            if key in self.attriblist_name:
                                appendfeatproperty(feat, 'name', value)
                            if key in self.attriblist_names:
                                appendfeatproperty(feat, 'names', value)
                            if key in self.attriblist_descr:
                                appendfeatproperty(feat, 'descr', value)


                    if len(feat['names']) > 200:
                        feat['names'] = feat['names'][0:195]+'...'
                    if len(feat['descr']) > 200:
                        feat['descr'] = feat['descr'][0:195]+'...'
                    self.features.append(feat)
            f.close()
            print('Tokens found: ' + ','.join([key for key in tokenMap]))

    def Process(self):

        #remove duplicates
        print('removing duplicates')
        dind={}
        featnr=0
        while featnr<len(self.features):
            feat=self.features[featnr]
            key=feat['seqid']+feat['id']
            if (key in dind):
                origfeat=self.features[dind[key]]
                origfeat['start']=min(origfeat['start'],feat['start'])
                origfeat['end']=max(origfeat['end'],feat['end'])
                self.features.pop(featnr)
            else:
                dind[key]=featnr
                featnr+=1

        print('building index')
        for i in xrange(len(self.features)):
            self.features[i]['nr']=i
            #Build an index
        self.featindex={}
        for feat in self.features:
            featid = feat['seqid']+feat['id']
            if featid in self.featindex:
                raise Exception('Duplicate feature: '+str(feat))
            self.featindex[featid]=feat['nr']

        #extending genes with exon regions
        print('extending')
        for feat in self.features:
            myfeat=feat
            if myfeat['type']==self.exonid:
                parentfeat=self.GetParentFeature(myfeat)
                if parentfeat!=None:
                    if parentfeat['end']<feat['end']:
#                        print('Right extending {0} from {1} to {2}'.format(parentfeat['id'],parentfeat['end'],feat['end']))
                        parentfeat['end']=feat['end']
                    if parentfeat['start']>feat['start']:
#                        print('Left extending {0} from {1} to {2}'.format(parentfeat['id'],parentfeat['start'],feat['start']))
                        parentfeat['start']=feat['start']



        #collect children of each feature
        for feat in self.features:
            myfeat=feat
            while self.GetParentFeature(myfeat)!=None:
                myparent=self.GetParentFeature(myfeat)
                myparent['children'].append(feat)
                myfeat=myparent
            myparent=self.GetParentFeature(feat)

    def save(self,filename):
        print('saving')
        typemap={}
        f=open(filename,'w')
        f.write('chromid\tfstart\tfstop\tfid\tfparentid\tftype\tfname\tfnames\tdescr\n')
        for feat in self.features:
            if not(feat['type'] in typemap):
                typemap[feat['type']]=0
            typemap[feat['type']]+=1
            if (feat['type'] in self.targetfeaturelist):
                f.write(feat['seqid']+'\t')
                f.write(str(feat['start'])+'\t')
                f.write(str(feat['end'])+'\t')
                f.write(feat['id']+'\t')
                f.write(''+'\t')
                f.write('gene'+'\t')
                f.write(feat['name']+'\t')
                f.write(feat['names']+'\t')
                f.write(feat['descr'])
                f.write('\n')
                for child in feat['children']:
                    if child['type']==self.exonid:
                        f.write(child['seqid']+'\t')
                        f.write(str(child['start'])+'\t')
                        f.write(str(child['end'])+'\t')
                        f.write(child['id']+'\t')
                        f.write(feat['id']+'\t')
                        f.write('CDS'+'\t') #CDS is the internally used identifier for an exon
                        f.write(child['name']+'\t\t\t')
                        f.write('\n')
        f.close()
        print(str(typemap))



#chromlist=range(1,15)
#basepath="C:/Data/Genomes/Plasmodium"
#filelist=['{0}/Pf3D7_{1}.gff'.format(basepath,str(nr).zfill(2)) for nr in chromlist]
#parser=GFFParser()
#parser.parseGFF(filelist)
#parser.Process()
#parser.save('{0}/features.txt'.format(basepath))


# basepath="C:/Data/Genomes/Plasmodium/Version3"
# filelist=['{0}/Pf3D7_v3.gff'.format(basepath)]
# parser=GFFParser()
# parser.targetfeaturelist=['repeat_region','pseudogene','snRNA','tRNA','centromere','pseudogenic_exon','pseudogenic_transcript','rRNA','snoRNA','polypeptide_motif','ncRNA']
# parser.parseGFF(filelist)
# parser.Process()
# parser.save('{0}/features.txt'.format(basepath))

basepath = '.'

#============= FAKE STUFF FOR DEBUGGING; REMOVE FOR PRODUCTION ==============
if False:
    basepath = '/home/pvaut/Documents/Genome/SourceData/datasets/Samples_and_Variants/refgenome'
#    sys.argv = ['', '10000', 'GTF', 'CDS', 'exon', 'gene_name', 'gene_name', 'description', 'annotation.gff']
    sys.argv = ['', 'all', 'GFF', 'gene,pseudogene', 'exon', 'Name', 'Name,Alias,previous_systematic_id', 'descr', 'annotation.gff']
#============= END OF FAKE STUFF ============================================


if len(sys.argv)<9:
    print('Usage: COMMAND maxrowcount format geneidlist exonid attrib_genename attrib_genenames, attrib_descr GFFFileName')
    sys.exit()

arg_maxrowcount = sys.argv[1]
arg_format = sys.argv[2]
arg_geneidlist = sys.argv[3]
arg_exonid = sys.argv[4]
arg_attrib_genename = sys.argv[5]
arg_attriblist_genenames = sys.argv[6]
arg_attrib_descr = sys.argv[7]
sourcefile = sys.argv[8]

if arg_format not in ['GFF', 'GTF']:
    raise Exception('Invalid format specifier (should be GFF or GTF): '+arg_format)

filelist=['{0}/{1}'.format(basepath,sourcefile)]
parser=GFFParser()
if arg_maxrowcount!='all':
    parser.maxrowcount = int(arg_maxrowcount)
parser.targetfeaturelist = arg_geneidlist.split(',')
parser.exonid = arg_exonid

parser.attriblist_name = arg_attrib_genename.split(',')
parser.attriblist_names = arg_attriblist_genenames.split(',')
parser.attriblist_descr = arg_attrib_descr.split(',')

if arg_format == 'GTF':
    parser.parseGTF(filelist)
else:
    parser.parseGFF(filelist)
parser.Process()
parser.save('{0}/annotation.txt'.format(basepath))

tb = VTTable.VTTable()
tb.allColumnsText = True
tb.LoadFile(basepath+'/annotation.txt')
tb.ConvertColToValue('fstart')
tb.ConvertColToValue('fstop')
print('DD>')
tb.PrintRows(0,19)
print('<DD')
tb.SaveSQLCreation(basepath+'/annotation_create.sql','annotation')
tb.SaveSQLDump(basepath+'/annotation_dump.sql','annotation')

