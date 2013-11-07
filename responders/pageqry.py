import B64
import DQXDbTools
import DQXUtils



def response(returndata):

    mytablename = returndata['tbname']
    encodedquery = returndata['qry']
    myorderfield = returndata['order']
    sortreverse = int(returndata['sortreverse']) > 0
    isdistinct = ('distinct' in returndata) and (int(returndata['distinct']) > 0)

    mycolumns=DQXDbTools.ParseColumnEncoding(returndata['collist'])

    databaseName=None
    if 'database' in returndata:
        databaseName = returndata['database']
    db = DQXDbTools.OpenDatabase(databaseName)
    cur = db.cursor()

    whc=DQXDbTools.WhereClause()
    whc.ParameterPlaceHolder='%s'#NOTE!: MySQL PyODDBC seems to require this nonstardard coding
    whc.Decode(encodedquery)
    whc.CreateSelectStatement()

    #print('---')
    #print(whc.querystring_params)
    #print('---')


    #Determine total number of records
    if int(returndata['needtotalcount'])>0:
        sqlquery="SELECT COUNT(*) FROM {0}".format(mytablename)
        if len(whc.querystring_params)>0:
            sqlquery+=" WHERE {0}".format(whc.querystring_params)
        print('   executing count query...')
        tm = DQXUtils.Timer()
        cur.execute(sqlquery,whc.queryparams)
        print('   finished in {0}s'.format(tm.Elapsed()))
        returndata['TotalRecordCount']=cur.fetchone()[0]



    #Fetch the actual data
    strrownr1,strrownr2=returndata['limit'].split('~')
    rownr1=int(0.5+float(strrownr1))
    rownr2=int(0.5+float(strrownr2))
    if rownr1<0: rownr1=0
    if rownr2<=rownr1: rownr2=rownr1+1

    sqlquery = "SELECT "
    if isdistinct:
        sqlquery = "SELECT DISTINCT "
    sqlquery += "{0} FROM {1}".format(','.join([x['Name'] for x in mycolumns]), mytablename)
    if len(whc.querystring_params)>0:
        sqlquery += " WHERE {0}".format(whc.querystring_params)
    if len(myorderfield)>0:
        sqlquery += " ORDER BY {0}".format(DQXDbTools.CreateOrderByStatement(myorderfield,sortreverse))
    sqlquery += " LIMIT {0}, {1}".format(rownr1,rownr2-rownr1+1)

    print('################################################')
    print('###QRY:'+sqlquery)
    print('###PARAMS:'+str(whc.queryparams))
    print('################################################')

    cur.execute(sqlquery,whc.queryparams)

    returndata['DataType']='Points'
    pointsx=[]
    yvalrange=range(0,len(mycolumns))
    pointsy=[]
    for ynr in yvalrange:
        pointsy.append([])
    rowidx=0
    for row in cur.fetchall() :
        pointsx.append(rownr1+rowidx)
        for ynr in yvalrange:
            if row[ynr]!=None:
                pointsy[ynr].append(row[ynr])
            else:
                pointsy[ynr].append(None)
        rowidx+=1

    valcoder=B64.ValueListCoder()
    returndata['XValues']=valcoder.EncodeIntegersByDifferenceB64(pointsx)
    for ynr in yvalrange:
        returndata[mycolumns[ynr]['Name']]=valcoder.EncodeByMethod(pointsy[ynr],mycolumns[ynr]['Encoding'])

    cur.close()
    db.close()

    return returndata
