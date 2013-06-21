import sys
import os
sys.path.append(os.path.dirname(__file__))
import wsgi_server

def application(environ, start_response):
    return wsgi_server.application(environ,start_response)
