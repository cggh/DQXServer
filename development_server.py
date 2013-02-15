import wsgi_server

#This implements an embedded web server for development & testing usage
#Production deployment should be based on a formal web server environment such as Apache2
try:
    from werkzeug.serving import run_simple
    def start(port):
        print "Serving HTTP using werkzeug on port "+str(port)
        run_simple('localhost', port, wsgi_server.application, use_reloader=True, threaded=True)

except ImportError:
    print "******** ERROR: Werkzeug not found falling back to wsgiref! **********************"
    print "WARNING: this server is known not to be fully stable, notably when accessed from IE"
    from wsgiref.simple_server import make_server
    def start(port):
        httpd = make_server('', port, wsgi_server.application)
        print "Serving HTTP using wsgiref on port "+str(port)
        httpd.serve_forever()


if __name__ == '__main__':
    start(8000)