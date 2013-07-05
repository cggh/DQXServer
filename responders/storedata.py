import DQXDbTools
import uuid

def response(returndata):

    environ=returndata['environ']
    request_body_size = int(environ['CONTENT_LENGTH'])
    request_body = environ['wsgi.input'].read(request_body_size)

    id = str(uuid.uuid1())

    db = DQXDbTools.OpenDatabase()
    cur = db.cursor()
    sqlstring = 'INSERT INTO storage (id,content) VALUES ("{0}","{1}")'.format(id, request_body)
    cur.execute(sqlstring)
    db.commit()

    returndata['id']=id
    return returndata