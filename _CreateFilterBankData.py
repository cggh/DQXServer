# This file is part of DQXServer - (C) Copyright 2014, Paul Vauterin, Ben Jeffery, Alistair Miles <info@cggh.org>
# This program is free software licensed under the GNU Affero General Public License.
# You can find a copy of this license in LICENSE in the top directory of the source code or at <http://opensource.org/licenses/AGPL-3.0>

import math
import random
import SummCreate
import sys

basedir='.'

#============= FAKE STUFF FOR DEBUGGING; REMOVE FOR PRODUCTION ==============
#basedir='C:\Data\Test\Genome'
#sys.argv=['','/home/pvaut/Documents/Genome/Tracks-PfPopGen3.1/Coverage2','Summ01']
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

