import base64

def ReturnFile(meta,returndata):
    f=open(meta['BASEDIR']+'/'+returndata['name']+'.txt')
    content=f.read()
    f.close()
    returndata['content']=base64.b64encode(content)
    return returndata
