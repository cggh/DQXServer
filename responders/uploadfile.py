import cgi
import os
import sys
import config
import uuid
import DQXUtils


def response(returndata):
    environ=returndata['environ']

    filesize = int(environ['CONTENT_LENGTH'])

    filename = str(uuid.uuid1())
    file_path = os.path.join(config.BASEDIR, 'Uploads', filename)

    readsize = 0
    with open(file_path, 'wb') as output_file:
        while readsize < filesize:
            blocksize = min(filesize-readsize, 1024)
            data = environ['wsgi.input'].read(blocksize)
            output_file.write(data)
            readsize += blocksize

    if filename:
        DQXUtils.LogServer('Uploaded file '+filename)
        returndata['filename'] = filename
    else:
        DQXUtils.LogServer('Failed to upload file')
        returndata['Error'] = 'Failed'


    return returndata
