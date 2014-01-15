import DQXDbTools
import uuid
import os
import config

def response(returndata):

    environ=returndata['environ']
    request_body_size = int(environ['CONTENT_LENGTH'])
    request_body = environ['wsgi.input'].read(request_body_size)

    id = str(uuid.uuid1())

    filename = os.path.join(config.BASEDIR, 'temp', 'store_'+id)
    with open(filename, 'w') as fp:
        fp.write(request_body)

    returndata['id']=id
    return returndata