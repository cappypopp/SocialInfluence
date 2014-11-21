__author__ = 'cappypopp'

import csv
import cStringIO
import codecs

class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")

class DictUnicodeWriter(object):
    """ csv writer that handles embedded Unicode characters

    implements same interface as DictWriter so same conventions apply
    """

    def __init__(self, f, fieldnames, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.DictWriter(self.queue, fieldnames, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, d):
        self.writer.writerow({k: unicode(v).encode("utf-8") for k, v in d.items()})
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and re-encode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for d in rows:
            self.writerow(d)

    def writeheader(self):
        self.writer.writeheader()

class DictUnicodeReader(object):
    """
    csv class that handles embedded unicode characters

    implements same interface as DictReader so same conventions apply
    """
    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        #self.reader = csv.reader(f, dialect=dialect, **kwds)
        self.reader = csv.DictReader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        #return [unicode(s, "utf-8") for s in row]
        outrow = dict([(k, unicode(v, "utf-8")) for k,v in row.iteritems()])
        return outrow

    def __iter__(self):
        return self
