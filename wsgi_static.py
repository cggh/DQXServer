import wsgi_server
import os
from werkzeug.wsgi import SharedDataMiddleware
import config
from cas import CASMiddleware
import logging
from werkzeug.contrib.sessions import FilesystemSessionStore
from werkzeug.wrappers import Response

#logging.basicConfig(level=logging.DEBUG)

#This function is called if:
# Not authenticated
# the ignore_redirect regex matches the (full) url pattern
def ignored_callback(environ, start_response):
    response = Response('{"Error":"NotAuthenticated"}')
#    response.status = '401 Unauthorized'
    response.status = '200 OK'
    response.headers['Content-Type'] = 'application/json'

    return response(environ, start_response)

application = wsgi_server.application

application = SharedDataMiddleware(application, {
    '/static': os.path.join(os.path.dirname(__file__), 'static')
})

try:
    cas_service = config.CAS_SERVICE
except AttributeError:
    cas_service = ''
if cas_service != '':
  fs_session_store = FilesystemSessionStore()
  application = CASMiddleware(application, cas_root_url = cas_service, logout_url = config.CAS_LOGOUT_PAGE, logout_dest = config.CAS_LOGOUT_DESTINATION, protocol_version = config.CAS_VERSION, casfailed_url = config.CAS_FAILURE_PAGE, entry_page = '/static/main.html', session_store = fs_session_store, ignore_redirect = '(.*)\?datatype=', ignored_callback = ignored_callback)
