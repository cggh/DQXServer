

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