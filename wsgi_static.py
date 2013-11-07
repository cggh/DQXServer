import wsgi_server
import os
from werkzeug.wsgi import SharedDataMiddleware

application = SharedDataMiddleware(wsgi_server.application, {
    '/static': os.path.join(os.path.dirname(__file__), 'static')
})
