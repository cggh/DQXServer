from urlparse import parse_qs
import DQXUtils
import simplejson
import responders


def application(environ, start_response):
    returndata = dict((k,v[0]) for k,v in parse_qs(environ['QUERY_STRING']).items())
    request_type = returndata['datatype']

    tm = DQXUtils.Timer()
    try:
        resp_func = getattr(responders, request_type)#Fetch the handfler by request type, using some introspection magic in responders/__init__.py
    except AttributeError:
        raise Exception("Unknown request {0}".format(request_type))
    response = resp_func(returndata)

    #todo: make the response handling part of the handler, to avoid this branching
    #This will become necessary when we have more handlers with different response types (e.g. other downloads)
    if request_type == "downloadtable":#Respond to a download request with a text attachment
        status = '200 OK'
        response_headers = [('Content-type', 'text/plain'),('Content-Disposition','attachment; filename=download.txt')]
        start_response(status, response_headers)
        for item in response:
            yield item
    else:#respond to any other event with json
        response = simplejson.dumps(response, use_decimal=True)
        status = '200 OK'
        response_headers = [('Content-type', 'application/json'),
                            ('Content-Length', str(len(response)))]
        start_response(status, response_headers)
        yield response

    print('@@@@ Responded to {0} in wall={1}s cpu={2}s'.format(request_type, tm.Elapsed(),tm.ElapsedCPU()))