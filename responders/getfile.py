import base64
import config

def response(returndata):
    filename=returndata['name']
    #!!!todo: add more integrity checks for the filename
    if filename.find('..')>=0:
        raise Exception('Invalid file name')
    f=open(config.BASEDIR+'/'+filename+'.txt')
    content=f.read()
    f.close()
    returndata['content']=base64.b64encode(content)
    return returndata
