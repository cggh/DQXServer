import DQXDbTools

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

    sqlquery="SELECT * FROM {0} WHERE {1}".format(
        mytablename,
        whc.querystring_params
    )

    cur.execute(sqlquery,whc.queryparams)
    therow=cur.fetchone()
    if therow is None:
        returndata['Error']='Record not found'
    else:
        data={}
        colnr=0
        for column in cur.description:
            data[column[0]]=str(therow[colnr])
            colnr += 1
        returndata['Data']=data

    return returndata
