import cgi
import os
import sys
import config
import uuid

#Based on https://github.com/thejimmyg/wsgi-file-upload

def response(returndata):
    environ=returndata['environ']

    post = cgi.FieldStorage(
        fp=environ['wsgi.input'],
        environ=environ,
        keep_blank_values=True
    )

    filename=None

    #if 'filedata' not in post:
    #    return 'missing filedata'
    #fileitem = post["filedata"]
    fileitem=post#Note: if file is send directly using XMLHttpRequest, this is the way to read it

    #print(str(post))


    if fileitem.file:
        #filename = fileitem.filename.decode('utf8').replace('\\','/').split('/')[-1].strip()
        #if not filename:
        #    raise Exception('No valid filename specified')
        filename = str(uuid.uuid1())
        file_path = os.path.join(config.BASEDIR, 'Uploads', filename)
        # Using with makes Python automatically close the file for you
        counter = 0
        with open(file_path, 'wb') as output_file:
            # In practice, sending these messages doesn't work
            # environ['wsgi.errors'].write('Receiving upload ...\n')
            # environ['wsgi.errors'].flush()
            # print 'Receiving upload ...\n'
            while 1:
                data = fileitem.file.read(1024)
                # End of file
                if not data:
                    break
                output_file.write(data)
                counter += 1
                if counter == 100:
                    counter = 0
                    # environ['wsgi.errors'].write('.')
                    # environ['wsgi.errors'].flush()
                    # print '.',

    if filename:
        print('Uploaded file '+filename)
        returndata['filename'] = filename
    else:
        print('Failed to upload file')
        returndata['Error'] = 'Failed'


    return returndata
