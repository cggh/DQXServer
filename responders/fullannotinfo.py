import DQXDbTools


def response(returndata):

    databaseName=None
    if 'database' in returndata:
        databaseName = returndata['database']
    db = DQXDbTools.OpenDatabase(DQXDbTools.ParseCredentialInfo(returndata), databaseName)
    cur = db.cursor()

    sqlquery="SELECT * FROM {tablename} WHERE fid='{id}'".format(
        tablename=DQXDbTools.ToSafeIdentifier(returndata['table']),
        id=DQXDbTools.ToSafeIdentifier(returndata['id'])
    )

    cur.execute(sqlquery)
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
