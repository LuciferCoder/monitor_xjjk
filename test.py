import hashlib

# pwd = 'pguU9#Cz'
#
# sha = hashlib.sha1(pwd.encode()).hexdigest()
# print(sha)
# sha = hashlib.sha1(pwd.encode()).digest()
# print(sha)
#
# sha_re='a52fdfd2cfce28792ca92bfa05675f16d61bbb7f'
#
# sha = hashlib.sha1(sha_re.encode()).digest()
# print(sha)
#

from datetime import datetime, timedelta

datenow = datetime.now()
datenowstring = datenow.strftime("%Y%m%d%H%M%S")

print(datenowstring)

print(str(datenowstring)[8:-2])



