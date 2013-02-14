from wsgiref.simple_server import make_server
import wsgi_server


httpd = make_server('', 8000, wsgi_server.application)
print "Serving HTTP on port 8000..."

# Respond to requests until process is killed
httpd.serve_forever()

# Alternative: serve one request, then exit
httpd.handle_request()