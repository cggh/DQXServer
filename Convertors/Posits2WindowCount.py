import sys
import os
import simplejson

basedir='.'

#============= FAKE STUFF FOR DEBUGGING; REMOVE FOR PRODUCTION ==============
if True:
    basedir='/home/pvaut/Documents/Genome/Ag'
    sys.argv=['','posits.txt', '150']
#============= END OF FAKE STUFF ============================================

if len(sys.argv)<3:
    print('Usage: COMMAND Sourcefilename halfwinsize')
    print('Source file contains chromosome-TAB-position (no header)')
    sys.exit()

sourcefilename = sys.argv[1]
hwinsize = int(sys.argv[2])



class Handler:
    def __init__(self, chromosome, hwinsize, ofl):
        print('##### Start processing chromosome '+chromosome)
        self.chromosome = chromosome
        self.hwinsize = hwinsize
        self.curwincent = self.hwinsize
        self.curwinend = 2*self.hwinsize
        self.ct = 0
        self.ofl = ofl


    def Add(self, posit):
        while posit > self.curwinend:
            self.WriteWindow()
            self.curwincent += 2*self.hwinsize+1
            self.curwinend += 2*self.hwinsize+1
            self.totct = 0
            self.ct = 0
        self.ct += 1

    def WriteWindow(self):
        self.ofl.write('{0}\t{1}\t{2}\n'.format(self.chromosome, self.curwincent, self.ct))


    def Finalise(self):
        self.WriteWindow()


ifile=open(basedir+'/'+sourcefilename,'r')

curchromoname=''
handler = None

ofl = open(basedir+'/wincount'+'.txt','w')

basect = 0
while True:
    line=ifile.readline()
    if not(line):
        break
    line=line.rstrip('\n')
    tokens=line.split('\t')
    chromoname=tokens[0]
    posit=int(tokens[1])
    if chromoname!=curchromoname:
        if handler != None:
            handler.Finalise()
        handler = Handler(chromoname, hwinsize, ofl)
        print('Reading chromosome {0}'.format(chromoname))
        curchromoname=chromoname
    handler.Add(posit)
    basect += 1
    if basect % 500000 == 0:
        print(str(basect))

if handler != None:
    handler.Finalise()

ifile.close()
ofl.close()

