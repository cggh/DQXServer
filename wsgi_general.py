import DQXUtils
import DQXDbTools

def application(environ, start_response):
    #For the root we do a relative redirect to index.html, hoping the app has one
    if environ['PATH_INFO'] == '/':
        start_response('301 Moved Permanently', [('Location', 'index.html'),])
        return

    with DQXDbTools.DBCursor() as cur:
        cur.execute('select id from datasetindex')
        datasets = [d[0] for d in cur.fetchall()]

    #Redirect to specific dataset
    path = environ['PATH_INFO'].split('/')
    if len(path) >= 2 and path[-2] in datasets and not (len(path) >= 3 and path[-3] == "Docs"):
        start_response('301 Moved Permanently', [('Location', '../index.html?dataset='+path[-2]),])
        return
    if path[-1] in datasets:
        start_response('301 Moved Permanently', [('Location', '../index.html?dataset='+path[-1]),])
        return

    #Everything else is 404
    DQXUtils.LogServer('404:' + environ['PATH_INFO'])
    start_response('404 Not Found', [])
    try:
        with open('static/404.html') as page:
            yield page.read()
    except IOError:
        yield '404 Page Not Found'
    return
