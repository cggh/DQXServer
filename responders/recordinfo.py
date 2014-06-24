# This file is part of DQXServer - (C) Copyright 2014, Paul Vauterin, Ben Jeffery, Alistair Miles <info@cggh.org>
# This program is free software licensed under the GNU Affero General Public License.
# You can find a copy of this license in LICENSE in the top directory of the source code or at <http://opensource.org/licenses/AGPL-3.0>

import DQXDbTools
import DQXUtils
from DQXDbTools import DBCOLESC
from DQXDbTools import DBTBESC

def response(returndata):
    mytablename = returndata['tbname']
    encodedquery = returndata['qry']

    databaseName = None
    if 'database' in returndata:
        databaseName = returndata['database']
    db = DQXDbTools.OpenDatabase(DQXDbTools.ParseCredentialInfo(returndata), databaseName)
    cur = db.cursor()

    whc = DQXDbTools.WhereClause()
    whc.ParameterPlaceHolder = '%s' #NOTE!: MySQL PyODDBC seems to require this nonstardard coding
    whc.Decode(encodedquery)
    whc.CreateSelectStatement()

    sqlquery = "SELECT * FROM {0} WHERE {1}".format(
        DBTBESC(mytablename),
        whc.querystring_params
    )

    if DQXDbTools.LogRequests:
        DQXUtils.LogServer('################################################')
        DQXUtils.LogServer('###QRY:'+sqlquery)
        DQXUtils.LogServer('###PARAMS:'+str(whc.queryparams))
        DQXUtils.LogServer('################################################')


    cur.execute(sqlquery, whc.queryparams)
    therow = cur.fetchone()
    if therow is None:
        returndata['Error'] = 'Record not found'
    else:
        data={}
        colnr=0
        for column in cur.description:
            data[column[0]] = str(therow[colnr])
            colnr += 1
        returndata['Data'] = data

    return returndata
