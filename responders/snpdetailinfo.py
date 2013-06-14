import base64
import config
import commands

def response(returndata):
    filename=config.BASEDIR+'/'+returndata['name']+'.vcf.gz'
    chrom=returndata['chrom']
    pos=returndata['pos']
    cmd='tabix {0} {1}:{2}-{2}'.format(filename,chrom,pos)
    returndata['cmd']=cmd
    output=commands.getstatusoutput(cmd)[1]
    returndata['content']=output
    return returndata
