import requests
res = requests.post('http://35.198.227.133:3000/api/session',
	headers = {"Content-Type": "application/json"},
json = {"username": "nguyentnt@ghn.vn",
"password": "uQH7xiJglM?2cO"}
)
assert res.ok == True
token = res.json()['id']
print(token)