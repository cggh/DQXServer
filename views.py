from django.http import HttpResponse
import simplejson
import responders
import DQXUtils
import traceback
import sys

#NOTE: on development server, call with
#http://localhost:8000/app01?chrom=1&start=0&stop=3000000


#Pack result data into a http response
def CreateHttpResponse(data):
    returnstr = simplejson.dumps(data)
    return HttpResponse(returnstr)

#Convert an url into a map of query items
def GetRequestQuery(request):
    map={}
    for key in request.REQUEST:
        map[key]=request.REQUEST[key]
    return map


def DownloadTable(meta,returndata):
    resp = HttpResponse(responders.DownloadTable_Generator(meta,returndata),mimetype='text/plain')
    resp['Content-Disposition'] = 'attachment; filename=download.txt'
    return resp


def index(request):

    meta={}
    meta['DBSRV']='localhost'
    meta['DBUSER']='root'
    meta['DBPASS']='1234'
    meta['DB']='world'
    meta['BASEDIR']='C:/Data/Test/Genome'



    returndata=GetRequestQuery(request)
    mydatatype=returndata['datatype']

    try:
        tm=DQXUtils.Timer()

        if mydatatype=="downloadtable":
            return DownloadTable(meta,returndata)

        resplist=responders.GetRespList()
        if not(mydatatype in resplist):
            raise Exception("Unknown request {0}".format(mydatatype))
        else:
            resp=resplist[mydatatype](meta,returndata)
        print('@@@@ Responded to {0} in {1}s'.format(mydatatype,tm.Elapsed()))
        return CreateHttpResponse(resp)


    except Exception, err:
        print("**************** EXCEPTION RAISED: "+str(err))#TODO: do some nice logging here...
        traceback.print_exc(file=sys.stdout)
        returndata['Error']=str(err)
        return CreateHttpResponse(returndata)
