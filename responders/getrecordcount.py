import B64
import DQXDbTools
import DQXUtils



def response(returndata):
    mytablename=returndata['tbname']
    encodedquery=returndata['qry']

    databaseName=None
    if 'database' in returndata:
        databaseName = returndata['database']
    db = DQXDbTools.OpenDatabase(DQXDbTools.ParseCredentialInfo(returndata), databaseName)
    cur = db.cursor()

    whc=DQXDbTools.WhereClause()
    whc.ParameterPlaceHolder='%s'#NOTE!: MySQL PyODDBC seems to require this nonstardard coding
    whc.Decode(encodedquery)
    whc.CreateSelectStatement()


    #Determine total number of records
    sqlquery="SELECT COUNT(*) FROM {0}".format(mytablename)
    if len(whc.querystring_params) > 0:
        sqlquery += " WHERE {0}".format(whc.querystring_params)
    # DQXUtils.LogServer('   executing count query...')
    tm = DQXUtils.Timer()
    cur.execute(sqlquery, whc.queryparams)
    # DQXUtils.LogServer('   finished in {0}s'.format(tm.Elapsed()))
    returndata['TotalRecordCount'] = cur.fetchone()[0]

    cur.close()
    db.close()

    return returndata
