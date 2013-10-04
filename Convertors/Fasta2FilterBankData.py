import sys
import os
import simplejson

basedir='.'

#============= FAKE STUFF FOR DEBUGGING; REMOVE FOR PRODUCTION ==============
if False:
    basedir='/Users/pvaut/Documents/Data/Genome/Ag'
    sys.argv=['','Anopheles-gambiae-PEST_CHROMOSOMES_AgamP3.fa']
#============= END OF FAKE STUFF ============================================

if len(sys.argv)<2:
    print('Usage: COMMAND Sourcefilename')
    print('   Sourcefilename= fasta file')
    sys.exit()

sourcefilename = sys.argv[1]

blockSizeStart = 1
blockSizeIncrFactor = 2
blockSizeMax = 10000




class Summariser:
    def __init__(self, chromosome, blockSizeStart, blockSizeIncrFactor, blockSizeMax, outputFolder):
        print('##### Start processing chromosome '+chromosome)
        self.chromosome = chromosome
        self.outputFolder = outputFolder
        self.lastpos=-1
        self.blockSizeStart = blockSizeStart
        self.blockSizeIncrFactor = blockSizeIncrFactor
        self.blockSizeMax = blockSizeMax
        self.levels = []
        blocksize = self.blockSizeStart
        while blocksize <= self.blockSizeMax:
            level = { 'blocksize':blocksize, 'currentblockend':blocksize }
            level['counts'] = {'A':0, 'C':0, 'G':0, 'T':0}
            level['outputfile'] = open(self.outputFolder+'/Summ_'+self.chromosome+'_'+str(blocksize), 'w')
            self.levels.append(level)
            blocksize *= self.blockSizeIncrFactor
        print(str(self.levels))
        self.pos = 0


    def Add(self, val):
        for level in self.levels:
            while self.pos>=level['currentblockend']:
                self.CloseCurrentBlock(level)
                self.StartNextBlock(level)
            if val in level['counts']:
                level['counts'][val] += 1
        self.pos += 1

    def CloseCurrentBlock(self, level):
        maxcount = 0
        maxbase = 'N'
        for base in ['A', 'C', 'G', 'T']:
            if level['counts'][base]>maxcount:
                maxcount = level['counts'][base]
                maxbase = base
        level['outputfile'].write(maxbase)


    def StartNextBlock(self, level):
        level['currentblockend'] += level['blocksize']
        level['counts'] = {'A':0, 'C':0, 'G':0, 'T':0}


    def Finalise(self):
        for level in self.levels:
            self.CloseCurrentBlock(level)
            level['outputfile'].close()


cnf={}

cnf["BlockSizeStart"] = 1
cnf["BlockSizeIncrFactor"] = 2
cnf["BlockSizeMax"] = blockSizeMax

cnf["Properties"] = [
	{ "ID": "Base", "Type": "String"}
]

cnf["Summarisers"] = [
    {
        "PropID": "Base",
        "IDExt": "avg",
        "Method": "MostFrequent",
        "Encoder": {
            "ID": "FixedString",
            "Len": 1
        }
    }
]

fp = open(basedir+'/Summ.cnf','w')
simplejson.dump(cnf,fp,indent=True)
fp.write('\n')
fp.close()




#create output directory if necessary
outputdir=os.path.join(basedir,'Summaries')
if not os.path.exists(outputdir):
    os.makedirs(outputdir)

#remove all summary files that correspond to this configuration
for filename in os.listdir(outputdir):
    if filename.startswith('Summ_'):
        os.remove(os.path.join(outputdir,filename))



ifile=open(basedir+'/'+sourcefilename,'r')

chromoname=''
summariser = None

basect = 0
while True:
    line=ifile.readline()
    if not(line):
        break
    line=line.rstrip('\n')
    if line[0]=='>':
        chromoname=line[1:].split(' ')[0]
        if summariser != None:
            summariser.Finalise()
        summariser = Summariser(chromoname, blockSizeStart, blockSizeIncrFactor, blockSizeMax, outputdir)
        print('Reading chromosome {0}'.format(chromoname))
        posit=0
    else:
        for base in line:
            basect += 1
            if basect % 100000 == 0:
                print(str(basect))
            summariser.Add(base.upper())

ifile.close()

if summariser != None:
    summariser.Finalise()
