# This file is part of DQXServer - (C) Copyright 2014, Paul Vauterin, Ben Jeffery, Alistair Miles <info@cggh.org>
# This program is free software licensed under the GNU Affero General Public License.
# You can find a copy of this license in LICENSE in the top directory of the source code or at <http://opensource.org/licenses/AGPL-3.0>

import sys
import os
import shutil
import SummCreate


if False:#Convert Fasta data
    basedir='C:/Data/Genomes/Plasmodium/Version3'
    ifilename=basedir+'/Pf3D7_v3.fa'

    ifile=open(ifilename,'r')

    chromoname=''
    while True:
        line=ifile.readline()
        if not(line):
            break
        line=line.rstrip('\n')
        if line[0]=='>':
            if chromoname:
                ofile.close()
            chromoname=line[1:]
            print('Reading chromosome {0}'.format(chromoname))
            ofile=open('C:/Data/Test/Genome/PfCross/Sequence/'+chromoname+'.txt','w')
            ofile.write('Position\tBase\n')
            posit=0
        else:
            if chromoname:
                for base in line:
                    posit+=1
                    ofile.write(str(posit))
                    ofile.write('\t')
                    ofile.write(base)
                    ofile.write('\n')

    ifile.close()
    ofile.close()


if False:##Convert tandem repeat data
    ifilename='C:/Data/Genomes/PfCrosses/InputData/Pf3D7_v3.fa.2.7.7.80.10.50.500.dat'
    ofilename='C:/Data/Genomes/PfCrosses/InputData/tandem.txt'

    ifile=open(ifilename,'r')
    ofile=open(ofilename,'w')

    chromoname=''
    while True:
        line=ifile.readline()
        if not(line):
            break
        line=line.rstrip('\n')
        if line.startswith('Sequence:'):
            chromoname=line.split(' ')[1]
            print('Reading chromosome {0}'.format(chromoname))
            for i in range(6):
                line=ifile.readline()
            pass
        else:
            if (len(line)>0) and chromoname:
                id=chromoname+'_'+line.split(' ')[0]
                ofile.write('{0} {1}'.format(chromoname,id))
                comps=line.split(' ')
                for i in range(14):
                    ofile.write(' {0}'.format(comps[i]))
                ofile.write('\n')

    ifile.close()
    ofile.close()


if False:#convert strange %GC format
    basedir='C:/Data/Genomes/PfCrosses/InputData/GCContent'
    ifilename=basedir+'/Pf3D7_v3.gc20.txt'

    ifile=open(ifilename,'r')
    #ofile=open(ofilename,'w')

    chromoname=''
    while True:
        line=ifile.readline()
        if not(line):
            break
        line=line.rstrip('\n')
        if line.startswith('= '):
            if chromoname:
                ofile.close()
            chromoname=line.split(' ')[1]
            print('Reading chromosome {0}'.format(chromoname))
            ofile=open('C:/Data/Test/Genome/PfCross/GCContent20/'+chromoname+'.txt','w')
            ofile.write('Position\tGCContent\n')
        else:
            if (len(line)>0) and chromoname:
                ofile.write(line)
                ofile.write('\n')

    ifile.close()
    ofile.close()

if False:#Convert Uniqueness data
    basedir='C:/Data/Genomes/PfCrosses/InputData/Uniqueness'
    ifilename=basedir+'/Pf3D7_v3.uniqueness.txt'

    ifile=open(ifilename,'r')

    chromoname=''
    while True:
        line=ifile.readline()
        if not(line):
            break
        line=line.rstrip('\n')
        comps=line.split('\t')
        if chromoname!=comps[0]:
            if chromoname:
                ofile.close()
            chromoname=comps[0]
            print('Reading chromosome {0}'.format(chromoname))
            ofile=open('C:/Data/Test/Genome/PfCross/Uniqueness/'+chromoname+'.txt','w')
            ofile.write('Position\tUniqueness\n')
        if (len(line)>0) and chromoname:
            ofile.write('{0}\t{1}\n'.format(comps[1],comps[2]))

    ifile.close()
    ofile.close()



if False:#Calculate & convert Depth data
    inputdir='C:/Data/Genomes/PfCrosses/InputData/BamFiles'
    outputdir='C:/Data/Test/Genome/PfCross/Coverage'

    for filename in os.listdir(inputdir):
        if filename.endswith('.bam'):
            name=filename.split('.')[0]

            odir=outputdir+'/'+name
            if not os.path.exists(odir):
                os.makedirs(odir)
                shutil.copyfile(outputdir+'/Summ01.cnf',odir+'/Summ01.cnf')

                cmd='c:/software/samtools/samtools.exe depth {0}/{1}.bam > {0}/tmp_depth.txt'.format(inputdir,name)
                cmd=cmd.replace('/','\\')
                print('RUN '+cmd)
                rs=os.system(cmd)
                print('completed')
                #convert to per-chromosome file
                ifile=open('{0}/tmp_depth.txt'.format(inputdir),'r')
                chromoname=''
                while True:
                    line=ifile.readline()
                    if not(line):
                        break
                    line=line.rstrip('\n')
                    comps=line.split('\t')
                    if chromoname!=comps[0]:
                        if chromoname:
                            ofile.close()
                        chromoname=comps[0]
                        print('Reading chromosome {0}'.format(chromoname))
                        ofile=open(odir+'/'+chromoname+'.txt','w')
                        ofile.write('Position\tCoverage\n')
                    if (len(line)>0) and chromoname:
                        ofile.write('{0}\t{1}\n'.format(comps[1],comps[2]))

                ifile.close()
                ofile.close()

                creat=SummCreate.Creator('C:/Data/Test/Genome','PfCross/Coverage/'+name,'Summ01')
                creat.Summarise()



if True:#Summarise BAMStats2
    outputdir='C:/Data/Test/Genome/PfCross/BAMStats2'

    names=['ERR012788','ERR012840','ERR019061','ERR019054']
    for name in names:
        print('****************** Handling {0} *******************'.format(name))
        creat=SummCreate.Creator('C:/Data/Test/Genome','PfCross/BAMStats2/'+name,'Summ01')
        creat.Summarise()
