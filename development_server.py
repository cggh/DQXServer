import wsgi_server
try:
    from werkzeug.serving import run_simple
    def start(port):
        run_simple('localhost', port, wsgi_server.application, use_reloader=True, threaded=True)
except ImportError:
    print "Werkzeug not found falling back to wsgiref!"
    from wsgiref.simple_server import make_server

    def start(port):
        httpd = make_server('', port, wsgi_server.application)
        print "Serving HTTP on port 8000..."

        # Respond to requests until process is killed
        httpd.serve_forever()


if __name__ == '__main__':
    start(8000)