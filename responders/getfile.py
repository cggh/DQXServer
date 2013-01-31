import base64
import config

def response(returndata):
    f=open(config.BASEDIR+'/'+returndata['name']+'.txt')
    content=f.read()
    f.close()
    returndata['content']=base64.b64encode(content)
    return returndata
