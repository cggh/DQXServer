
import simplejson
import base64
import MySQLdb
import config

# Enumerates types of actions that can be done on a database entity
class DbOperationType:
    read = 1
    write = 2


# Encapsulates an operation that is done on a database entity
class DbOperation:

    def __init__(self, operationType, databaseName, tableName=None, columnName=None):
        if (databaseName is None) or (databaseName == ''):
            databaseName = config.DB
        self.operationType = operationType
        self.databaseName = databaseName
        self.tableName = tableName
        self.columnName = columnName

    def IsModify(self):
        return self.operationType == DbOperationType.write

    def OnDatabase(self, databaseName):
        return self.databaseName == databaseName

    def OnTable(self, tableName):
        return self.tableName == tableName

    def OnColumn(self, columnName):
        return self.columnName == columnName

    def __str__(self):
        st = ''
        if (self.operationType == DbOperationType.read):
            st += 'Read'
        if (self.operationType == DbOperationType.write):
            st += 'Write'
        st += ':'
        st += self.databaseName
        if self.tableName is not None:
            st += ':' + self.tableName
        if self.columnName is not None:
            st += ':' + self.columnName
        return st


# Encapsulates a read operation that is done on a database entity
class DbOperationRead(DbOperation):
    def __init__(self, databaseName, tableName=None, columnName=None):
        DbOperation.__init__(self, DbOperationType.read, databaseName, tableName, columnName)


# Encapsulates a write operation that is done on a database entity
class DbOperationWrite(DbOperation):
    def __init__(self, databaseName, tableName=None, columnName=None):
        DbOperation.__init__(self, DbOperationType.write, databaseName, tableName, columnName)


# Encapsulates the result of an authorisation request on a database operation
class DbAuthorization:
    def __init__(self, granted, reason=None):
        self.granted = granted
        if reason is None:
            if not granted:
                reason = 'Insufficient privileges to perform this action.'
            else:
                reason = ''
        self.reason = reason
    def IsGranted(self):
        return self.granted
    def __str__(self):
        return self.reason
    def __nonzero__(self):
        return self.granted
    def __bool__(self):
        return self.granted


# Define a custom credential handler here by defining function taking a DbOperation and a CredentialInformation
# returning a DbAuthorization instance
DbCredentialVerifier = None


class CredentialException(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)

class CredentialDatabaseException(CredentialException):
    def __init__(self, operation, auth):
        st = str(auth) + " \n\n[" + str(operation) + ']'
        CredentialException.__init__(self, st)



# Encapsulates information about the credentials a user has
class CredentialInformation:
    def __init__(self):
        self.clientaddress = None
        self.userid = 'anonymous'
        self.groupids = []

    def ParseFromReturnData(self, requestData):
        if 'environ' not in requestData:
            raise Exception('Data does not contain environment information')
        environ = requestData['environ']
        #print('ENV:'+str(environ))

        if 'REMOTE_ADDR' in environ:
            self.clientaddress = environ['REMOTE_ADDR']
        if 'REMOTE_USER' in environ:
            self.userid = environ['REMOTE_USER']
        if 'HTTP_CAS_MEMBEROF' in environ:
            for groupStr in environ['HTTP_CAS_MEMBEROF'].split(';'):
                groupPath = []
                for tokenStr in groupStr.split(','):
                    tokenid = tokenStr.split('=')[0]
                    tokencontent = tokenStr.split('=')[1]
                    if (tokenid == 'cn') or (tokenid == 'ou'):
                        groupPath.append(tokencontent)
                self.groupids.append('.'.join(groupPath.reversed()))


    # operation is of type DbOperation
    def CanDo(self, operation):
        if DbCredentialVerifier is not None:
            auth = DbCredentialVerifier(self, operation)
            return auth.IsGranted()
        else:
            return True

    # operation is of type DbOperation. raises an exception of not authorised
    def VerifyCanDo(self, operation):
        if DbCredentialVerifier is not None:
            auth = DbCredentialVerifier(self, operation)
            if not(auth.IsGranted()):
                raise CredentialDatabaseException(operation, auth)



# Create a credential info instance from a DQXServer request data environment
def ParseCredentialInfo(requestData):
    cred = CredentialInformation()
    cred.ParseFromReturnData(requestData)
    return cred








def OpenDatabase(credInfo, database=None):
    if (database is None) or (database == ''):
        database = config.DB
    credInfo.VerifyCanDo(DbOperationRead(database))
    return MySQLdb.connect(host=config.DBSRV, user=config.DBUSER, passwd=config.DBPASS, db=database, charset='utf8')

def OpenNoDatabase(credInfo):
    return MySQLdb.connect(host=config.DBSRV, user=config.DBUSER, passwd=config.DBPASS, charset='utf8')


def ToSafeIdentifier(st):
    st = str(st)
    removelist=['"', "'", ';', '(', ')']
    for it in removelist:
        st=st.replace(it, "")
    return st


#parse column encoding information
def ParseColumnEncoding(columnstr):
    mycolumns=[]
    for colstr in columnstr.split('~'):
        mycolumns.append( { 'Encoding':colstr[0:2], 'Name':ToSafeIdentifier(colstr[2:]) } )
    return mycolumns


#A whereclause encapsulates the where statement of a single table sql query
class WhereClause:
    def __init__(self):
        self.query=None#this contains a tree of statements
        self.ParameterPlaceHolder="?"#determines what is the placeholder for a parameter to be put in an sql where clause string

    #Decodes an url compatible encoded query into the statement tree
    def Decode(self, encodedstr):
        encodedstr=encodedstr.replace("-","+")
        encodedstr=encodedstr.replace("_","/")
        decodedstr=base64.b64decode(encodedstr)
        self.query=simplejson.loads(decodedstr)
        pass

    #Creates an SQL where clause string out of the statement tree
    def CreateSelectStatement(self):
        self.querystring=''#will hold the fully filled in standalone where clause string (do not use this if sql injection is an issue!)
        self.querystring_params=''#will hold the parametrised where clause string
        self.queryparams=[]#will hold a list of parameter values
        self._CreateSelectStatementSub(self.query)
        #return(self.querystring)

    def _CreateSelectStatementSub_Compound(self,statm):
        if not(statm['Tpe'] in ['AND', 'OR']):
            raise Exception("Invalid compound statement {0}".format(statm['Tpe']))
        first=True
        for comp in statm['Components']:
            if not first:
                self.querystring+=" "+statm['Tpe']+" "
                self.querystring_params+=" "+statm['Tpe']+" "
            self.querystring+="("
            self.querystring_params+="("
            self._CreateSelectStatementSub(comp)
            self.querystring+=")"
            self.querystring_params+=")"
            first=False

    def _CreateSelectStatementSub_Comparison(self,statm):
        #TODO: check that statm['ColName'] corresponds to a valid column name in the table (to avoid SQL injection)
        if not(statm['Tpe'] in ['=', '<>', '<', '>', '<=', '>=', '!=', 'LIKE', 'CONTAINS', 'NOTCONTAINS', 'STARTSWITH', 'ISPRESENT', 'ISABSENT', '=FIELD', '<>FIELD', '<FIELD', '>FIELD', 'between']):
            raise Exception("Invalid comparison statement {0}".format(statm['Tpe']))

        processed=False


        if statm['Tpe']=='ISPRESENT':
            processed=True
            st='{0} IS NOT NULL'.format(statm['ColName'])
            self.querystring+=st
            self.querystring_params+=st

        if statm['Tpe']=='ISABSENT':
            processed=True
            st='{0} IS NULL'.format(statm['ColName'])
            self.querystring+=st
            self.querystring_params+=st

        if statm['Tpe']=='=FIELD':
            processed=True
            st='{0}={1}'.format(ToSafeIdentifier(statm['ColName']),ToSafeIdentifier(statm['ColName2']))
            self.querystring+=st
            self.querystring_params+=st

        if statm['Tpe']=='<>FIELD':
            processed=True
            st='{0}<>{1}'.format(ToSafeIdentifier(statm['ColName']),ToSafeIdentifier(statm['ColName2']))
            self.querystring+=st
            self.querystring_params+=st

        if (statm['Tpe']=='<FIELD') or (statm['Tpe']=='>FIELD'):
            processed=True
            operatorstr=statm['Tpe'].split('FIELD')[0]
            self.querystring+='{0} {4} {1} * {2} + {3}'.format(ToSafeIdentifier(statm['ColName']),ToSafeIdentifier(statm['Factor']),ToSafeIdentifier(statm['ColName2']),ToSafeIdentifier(statm['Offset']),operatorstr)
            self.querystring_params+='{0} {4} {1} * {2} + {3}'.format(ToSafeIdentifier(statm['ColName']),self.ParameterPlaceHolder,ToSafeIdentifier(statm['ColName2']),self.ParameterPlaceHolder,operatorstr)
            self.queryparams.append(ToSafeIdentifier(statm['Factor']))
            self.queryparams.append(ToSafeIdentifier(statm['Offset']))

        if statm['Tpe'] == 'between':
            processed = True
            self.querystring += ToSafeIdentifier(statm['ColName'])+' between '+ToSafeIdentifier(statm["CompValueMin"])+' and '+ToSafeIdentifier(statm["CompValueMax"])
            self.querystring_params += '{0} between {1} and {1}'.format(ToSafeIdentifier(statm['ColName']), self.ParameterPlaceHolder)
            self.queryparams.append(statm["CompValueMin"])
            self.queryparams.append(statm["CompValueMax"])

        if not(processed):
            decoval=statm['CompValue']
            operatorstr=statm['Tpe']
            if operatorstr=='CONTAINS':
                operatorstr='LIKE'
                decoval='%{0}%'.format(decoval)
            if operatorstr=='NOTCONTAINS':
                operatorstr='NOT LIKE'
                decoval='%{0}%'.format(decoval)
            if operatorstr=='STARTSWITH':
                operatorstr='LIKE'
                decoval='{0}%'.format(decoval)
            self.querystring+=ToSafeIdentifier(statm['ColName'])+' '+ToSafeIdentifier(operatorstr)+' '
            self.querystring_params+='{0} {1} {2}'.format(ToSafeIdentifier(statm['ColName']),ToSafeIdentifier(operatorstr),self.ParameterPlaceHolder)
            needquotes= (type(decoval) is not float) and (type(decoval) is not int)
            if needquotes:
                self.querystring+="'"
            self.querystring+=str(decoval)
            if needquotes:
                self.querystring+="'"
            self.queryparams.append(decoval)

    def _CreateSelectStatementSub(self,statm):
        if statm['Tpe']=='':
            return#trivial query
        self.querystring+="("
        self.querystring_params+="("
        if (statm['Tpe']=='AND') or (statm['Tpe']=='OR'):
            self._CreateSelectStatementSub_Compound(statm)
        else:
            self._CreateSelectStatementSub_Comparison(statm)
        self.querystring+=")"
        self.querystring_params+=")"





#unpacks an encoded 'order by' statement into an SQL statement
def CreateOrderByStatement(orderstr,reverse=False):
    dirstr=""
    if reverse: dirstr=" DESC"
    #note the following sql if construct is used to make sure that sorting always puts absent values at the end, which is what we want

    ### !!! todo: make this choice dependent on client
    # option 1 = better, slower (absent appear beneath)
    # opten 2 = sloppier, faster
#    return ', '.join( [ "IF(ISNULL({0}),1,0),{0}{1}".format(field,dirstr) for field in orderstr.split('~') ] )
    return ', '.join( [ "{0}{1}".format(field,dirstr) for field in orderstr.split('~') ] )
