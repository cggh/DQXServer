import sys
import os
sys.path.append(os.path.dirname(__file__))

from urlparse import parse_qs
import DQXUtils
import simplejson
import responders


def application(environ, start_response):
    returndata = dict((k,v[0]) for k,v in parse_qs(environ['QUERY_STRING']).items())
    request_type = returndata['datatype']

    tm = DQXUtils.Timer()
    try:
        resp_func = getattr(responders, request_type)
    except AttributeError:
        raise Exception("Unknown request {0}".format(request_type))
    response = resp_func(returndata)

    print('@@@@ Handling {0}'.format(request_type))
    if request_type == "downloadtable":
        status = '200 OK'
        response_headers = [('Content-type', 'text/plain'),('Content-Disposition','attachment; filename=download.txt')]
        start_response(status, response_headers)
        for item in response:
            yield item
        response['Content-Disposition'] = 'attachment; filename=download.txt'
    else:
        response = simplejson.dumps(response,use_decimal=True)
        status = '200 OK'
        response_headers = [('Content-type', 'application/json'),
                            ('Content-Length', str(len(response)))]
        start_response(status, response_headers)
        yield response
    print('@@@@ Responded to {0} in {1}s'.format(request_type, tm.Elapsed()))
