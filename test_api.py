import pycurl
from io import StringIO
from urllib.parse import urlencode
import certifi
import os

buf = StringIO()
data = {
    'token': os.environ.get('REDCAP_HCW_TOKEN'), # TODO: hide the token's value
    'content': 'report',
    'format': 'json',
    'report_id': '24219',
    'rawOrLabel': 'label',
    'rawOrLabelHeaders': 'raw',
    'exportCheckboxLabel': 'true',
    'returnFormat': 'json'
}
pf = urlencode(data)
ch = pycurl.Curl()
# to avoid "unable to get local issuer certificate" (https://stackoverflow.com/questions/16192832/pycurl-https-error-unable-to-get-local-issuer-certificate)
ch.setopt(pycurl.CAINFO, certifi.where())
# TODO: store URL in the config
ch.setopt(ch.URL, 'https://redcap.mountsinai.org/redcap/api/')
ch.setopt(ch.POSTFIELDS, pf)
ch.perform()
ch.close()
print(buf.getvalue())
buf.close()