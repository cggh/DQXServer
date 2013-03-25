import math
import random
import SummCreate
import sys

basedir='.'

#============= FAKE STUFF FOR DEBUGGING; REMOVE FOR PRODUCTION ==============
#basedir='C:\Data\Test\Genome'
#sys.argv=['','Test','Summ01']
#============= END OF FAKE STUFF ============================================


if len(sys.argv)<3:
    print('Usage: COMMAND DataFolder ConfigFilename')
    print('   DataFolder= folder containing the source data, relative to the current path')
    print('   ConfigFilename= name of the source configuration file (do not provide the extension ".cnf").')
    sys.exit()


dataFolder=sys.argv[1]
summaryFile=sys.argv[2]




creat=SummCreate.Creator(basedir,dataFolder,summaryFile)
creat.Summarise()

