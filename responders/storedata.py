# This file is part of DQXServer - (C) Copyright 2014, Paul Vauterin, Ben Jeffery, Alistair Miles <info@cggh.org>
# This program is free software licensed under the GNU Affero General Public License.
# You can find a copy of this license in LICENSE in the top directory of the source code or at <http://opensource.org/licenses/AGPL-3.0>

import DQXDbTools
import uuid

def response(returndata):

    environ=returndata['environ']
    request_body_size = int(environ['CONTENT_LENGTH'])
    request_body = environ['wsgi.input'].read(request_body_size)

    id = str(uuid.uuid1())

    credInfo = DQXDbTools.ParseCredentialInfo(returndata)
    db = DQXDbTools.OpenDatabase(credInfo)
    cur = db.cursor()
    sqlstring = 'INSERT INTO storage (id,content) VALUES ("{0}","{1}")'.format(id, request_body)
    cur.execute(sqlstring)
    db.commit()

    returndata['id']=id
    return returndata