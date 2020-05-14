import pycurl
from io import StringIO, BytesIO
import io
from urllib.parse import urlencode
import certifi
import os

buf =  BytesIO()  # StringIO()  #
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
ch.setopt(ch.URL, 'https://redcap.mountsinai.org/redcap/api/')
ch.setopt(ch.POSTFIELDS, pf)
# ch.setopt(ch.WRITEFUNCTION, buf.write)
ch.setopt(ch.WRITEDATA, buf)
ch.setopt(pycurl.CAINFO, certifi.where())
ch.perform()
ch.close()

# print(buf.getvalue())
output = buf.getvalue()
buf.seek(0)
output1 = buf.read()
print(output)
print(output1)
out_text = output.decode('UTF-8')
# print (output.read())  #'bytes' object has no attribute 'read'

buf.close()