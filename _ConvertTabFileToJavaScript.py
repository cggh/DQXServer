# This file is part of DQXServer - (C) Copyright 2014, Paul Vauterin, Ben Jeffery, Alistair Miles <info@cggh.org>
# This program is free software licensed under the GNU Affero General Public License.
# You can find a copy of this license in LICENSE in the top directory of the source code or at <http://opensource.org/licenses/AGPL-3.0>



filename='C:/Data/Genomes/PfCrosses/InputData/BamFiles/qcmeta-annotated.txt'
f=open(filename,'r')
columns=f.readline().rstrip('\n').split('\t')

coldict={columns[i]:i for i in range(len(columns))}

savecols=['project_code','ox_code','source_code','ena_run_accession','coverage']

for line in f:
    cells=line.rstrip('\n').split('\t')
    st=''
    for savecol in savecols:
        if st:
            st+=', '
        st+=savecol+':"'+cells[coldict[savecol]]+'"'
    st='{'+st+'}, '
    print(st)

pass