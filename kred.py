__author__ = 'cappy'
__all__ = ["Kred", "KredError", "KredHTTPError"]

try:
    import urllib.request as urllib_request
    import urllib.error as urllib_error
    import urllib.parse as urllib_parse
except ImportError:
    import urllib2 as urllib_request
    import urllib2 as urllib_error
    import urllib as urllib_parse

try:
    from cStringIO import StringIO
except ImportError:
    from io import BytesIO as StringIO

import gzip

try:
    import json
except ImportError:
    import simplejson as json

import socket

class _DEFAULT(object):
    pass

class KredError(Exception):
    """
    Base exception thrown when there is a
    general error with Kred API
    """
    def __init__(self,e):
        self.e = e

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return ("ERROR: %e" % self.e)

class KredHTTPError(KredError):
    """
    HTTP error thrown when interacting with api
    """
    def __init__(self, e, uri):
        self.e = e
        self.uri = uri

class KredCall( object ):

    def __init__(self, key, domain,
            callable_cls, api_version = "",
            uri = "", uriparts = None, secure=False):

        self.key = key
        self.domain = domain
        self.api_version = api_version
        self.callable_cls = callable_cls
        self.uri = uri
        self.secure = secure
        self.uriparts = uriparts

        def __getattr__(self, k):
            try:
                return object.__getattr__(self, k)
            except AttributeError:
                def extend_call(arg):
                    return self.callable_cls(
                        key=self.key, domain=self.domain,
                        api_version=self.api_version,
                        callable_cls=self.callable_cls, secure=self.secure,
                        uriparts=self.uriparts + (arg,))
                if k == "_":
                    return extend_call
                else:
                    return extend_call(k)

        def __call__(self, **kwargs):
            # Build the uri.
            uriparts = []
            api_version = self.api_version
            resource = "%s.json" % self.uriparts[0]

            uriparts.append(api_version)
            uriparts.append(resource)

            params = {}
            if self.key:
                params['key'] = self.key

            timeout = kwargs.pop('timeout', None)

            # append input variables
            for k, v in kwargs.items():
                if k == 'screenName':
                    uriparts.append('twitter')
                    params[k] = v
                elif k == 'kloutId':
                    uriparts.append(str(v))
                else:
                    uriparts.append(k)
                    uriparts.append(str(v))

            for uripart in self.uriparts[1:]:
                if not uripart == 'klout':
                    uriparts.append(str(uripart))

            uri = '/'.join(uriparts)
            if len(params) > 0:
                uri += '?' + urllib_parse.urlencode(params)

            secure_str = ''
            if self.secure:
                secure_str = 's'

            uriBase = "http%s://%s/%s" % (
                secure_str, self.domain, uri)

            headers = {'Accept-Encoding': 'gzip'}

            req = urllib_request.Request(uriBase, headers=headers)

            return self._handle_response(req, uri, timeout)

        def _handle_response(self, req, uri, timeout=None):
            kwargs = {}
            if timeout:
                socket.setdefaulttimeout(timeout)
            try:
                handle = urllib_request.urlopen(req)
                if handle.info().get('Content-Encoding') == 'gzip':
                    # Handle gzip decompression
                    buf = StringIO(handle.read())
                    f = gzip.GzipFile(fileobj=buf)
                    data = f.read()
                else:
                    data = handle.read()

                res = json.loads(data.decode('utf8'))
                return res
            except urllib_error.HTTPError:
                import sys
                _, e, _ = sys.exc_info()
                raise KredHTTPError( e, uri)

    #kredURL = "http://api.kred.com/kredscore?term=" & twitterName & "&source=twitter&app_id=068e96a1&app_key=a9b28a34c7a54268faf3af656cecd2a8"

class Kred(KredCall):

    def __init__(self, key,
                 domain="api.kred.com",
                 secure=False):

        #if api_version is _DEFAULT:
        #    api_version = "v2"

        KredCall.__init__(
            self, key=key, domain = domain,
            callable_cls=KredCall, secure=secure,
            uriparts=())