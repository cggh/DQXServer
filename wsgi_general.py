import DQXUtils


def application(environ, start_response):
    #For the root we do a relative redirect to index.html, hoping the app has one
    if environ['PATH_INFO'] == '/':
        start_response('301 Moved Permanently', [('Location', 'index.html'),])
        return

    DQXUtils.LogServer('404:' + environ['PATH_INFO'])
    start_response('404 Not Found', [])
    try:
        with open('static/404.html') as page:
            yield page.read()
    except IOError:
        yield '404 Page Not Found'
    return