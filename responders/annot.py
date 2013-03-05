import B64
import DQXDbTools
import config

#Return annotation information for a chromosome region
def response(returndata):

    databaseName=None
    if 'database' in returndata:
        databaseName = returndata['database']
    db = DQXDbTools.OpenDatabase(databaseName)
    cur = db.cursor()

    tablename=DQXDbTools.ToSafeIdentifier(returndata['table'])

    typequerystring='(true)'
    if len(returndata['ftype'])>0:
        typequerystring='(ftype="{0}")'.format(returndata['ftype'])
    if returndata['subfeatures']=='1':
        typequerystring+=' or (ftype="{1}")'.format(returndata['ftype'],returndata['fsubtype'])
    statement='SELECT fstart, fstop, fname, fid, ftype, fparentid FROM {tablename} WHERE ({typequery}) and (chromid="{chromid}") and (fstop>={start}) and (fstart<={stop}) ORDER BY fstart'.format(
        typequery=typequerystring,
        tablename=tablename,
        chromid=DQXDbTools.ToSafeIdentifier(returndata['chrom']),
        start=str(int(returndata['start'])),
        stop=str(int(returndata['stop']))
    )

    print(statement+'\n')

    cur.execute(statement)
    starts=[]
    stops=[]
    names=[]
    ids=[]
    types=[]
    parentids=[]
    for row in cur.fetchall() :
        starts.append(float(row[0]))
        stops.append(float(row[1]))
        name=row[2]
        id=row[3]
        tpe=row[4]
        parentid=row[5]
        names.append(name)
        ids.append(id)
        types.append(tpe)
        parentids.append(parentid)

    returndata['DataType']='Points'
    valcoder=B64.ValueListCoder()
    returndata['Starts']=valcoder.EncodeIntegersByDifferenceB64(starts)
    returndata['Sizes']=valcoder.EncodeIntegers([x[1]-x[0] for x in zip(starts,stops)])
    returndata['Names']=valcoder.EncodeStrings(names)
    returndata['IDs']=valcoder.EncodeStrings(ids)
    returndata['Types']=valcoder.EncodeStrings(types)
    returndata['ParentIDs']=valcoder.EncodeStrings(parentids)
    return returndata
