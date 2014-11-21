__author__ = 'cappy'

from lxml import html

result = []

urls = [
    'http://www8.hp.com/us/en/business-services/it-services.html?compURI=1079292',
    'http://www8.hp.com/us/en/business-services/it-services.html?compURI=1380514',
   'http://www8.hp.com/us/en/business-services/it-services.html?compURI=1078906',
   'http://www8.hp.com/us/en/business-services/it-services.html?compURI=1078604',
   'http://www8.hp.com/us/en/business-services/it-services.html?compURI=1079286',
   'http://www8.hp.com/us/en/business-services/it-services.html?compURI=1198280',
   'http://www8.hp.com/us/en/business-services/it-services.html?compURI=1078006',
   'http://www8.hp.com/us/en/business-services/it-services.html?compURI=1240568',
   'http://www8.hp.com/us/en/business-services/it-services.html?compURI=1325114',
   'http://www8.hp.com/us/en/business-services/it-services.html?compURI=1198271',
   'http://www8.hp.com/us/en/business-services/it-services.html?compURI=1078624',
   'http://www8.hp.com/us/en/business-services/it-services.html?compURI=1077422',
   'http://www8.hp.com/us/en/business-services/it-services.html?compURI=1078865'
]

for url in urls:
    t = html.parse(url)
    result.append(t.find(".//h1").text)

print '\n'.join(result)
