import math
import random
import SummCreate

basedir='C:/Data/Test/Genome'

def GenerateData(filename):
    random.seed(0)
    colrange=range(5)

    filename=basedir+'/SRC_'+filename+'.txt'
    f=open(filename,'w')
    f.write('Position\tcov\tqual\n')
    for i in xrange(300000):
        if i==100000: i=170000
        f.write(str(i+1))

        #coverage
        f.write('\t')
        vl=i/10000.0
        vl=50+20*math.sin(vl*vl)+10*math.sin(vl*7)+10*math.sin(vl*40)
        for colnr in colrange:
            vl1=vl+random.gauss(0,4)
            if random.random()<0.05:
                vl1+=random.gauss(0,20)
            vl1=max(1,vl1)
            if colnr>0:
                f.write(',')
            f.write(str(vl1))

        #quality
        f.write('\t')
        vl=50+20*math.sin(i/10000.0)+10*math.sin(i/150.0)+10*math.sin(i/2.0)
        vl1=vl+random.gauss(0,10)
        f.write(str(vl1))


        f.write('\n')
    f.close()



#id='Pf3D7_04'

#GenerateData(id)

#provider=DataProvider_File(basedir+'/CROSS/SRC_'+id+'.txt')
#creat=SummCreate.Creator('CROSS')
#creat.Summarise(id,provider)


#filename='ERR019061'
filename='Pf3D7_v3_hifi'
creat=SummCreate.Creator('C:/Data/Test/Genome','PfCross/BAMStats2/'+filename,'Summ01')
creat.Summarise()

