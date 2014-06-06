# This file is part of DQXServer - (C) Copyright 2014, Paul Vauterin, Ben Jeffery, Alistair Miles <info@cggh.org>
# This program is free software licensed under the GNU Affero General Public License.
# You can find a copy of this license in LICENSE in the top directory of the source code or at <http://opensource.org/licenses/AGPL-3.0>

import wsgi_static

print('Starting development server')
#This implements an embedded web server for development & testing usage
#Production deployment should be based on a formal web server environment such as Apache2
try:
    from werkzeug.serving import run_simple
    from werkzeug.wsgi import SharedDataMiddleware
    def start(port):
        print "Serving HTTP using werkzeug on port "+str(port)
        app = SharedDataMiddleware(wsgi_static.application, {
            '/static': 'PATH_TO_YOUR_APP'
        })
        run_simple('0.0.0.0', port, app, use_reloader=True, threaded=True)
except ImportError:
    print "******** ERROR: Werkzeug not found falling back to wsgiref! **********************"
    print "WARNING: this server is known not to be fully stable, notably when accessed from IE"
    from wsgiref.simple_server import make_server
    def start(port):
        httpd = make_server('', port, wsgi_static.application)
        print "Serving HTTP using wsgiref on port "+str(port)
        httpd.serve_forever()


if __name__ == '__main__':
    start(8000)