import DQXDbTools

def response(returndata):
    databaseName = returndata['database']
    credInfo = DQXDbTools.ParseCredentialInfo(returndata)
    returndata['read'] = credInfo.CanDo(DQXDbTools.DbOperationRead(databaseName))
    returndata['write'] = credInfo.CanDo(DQXDbTools.DbOperationWrite(databaseName))
