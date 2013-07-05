import DQXDbTools
import uuid

#Find hits for gene patterns (or similar searches)
def response(returndata):

    id = returndata['id']
    db = DQXDbTools.OpenDatabase()
    cur = db.cursor()
    sqlstring = 'SELECT content FROM storage WHERE id="{0}"'.format(id)
    cur.execute(sqlstring)

    therow = cur.fetchone()
    if therow is None:
        returndata['Error'] = 'Storage record not found'
    else:
        returndata['content'] = therow[0]

    return returndata