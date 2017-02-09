# appengine-flask-wells
```
Edit the main.py file and update the <YOUR PROJECT ID> with your project id
```
## setup
```
virtualenv new_venv

# windows
new_venv\scripts\activate.bat

# bash

pip install -r requirements.txt

```
## deploy
```
pip install -r requirements.txt -t lib
gcloud app deploy --project=data-managers-search --version=v1 ./wells.yaml

#browse to
(service) https://guestbook-dot-<projectname>.appspot.com
(default) https://<projectname>.appspot.com
```
