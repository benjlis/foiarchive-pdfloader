# Installation

1. clone the repo: git clone https://github.com/benjlis/foiarchive-pdfloader.git
2. cd foiarchive-pdfloader
3. create a virtual environment: python3 -m venv env
4. activate the envionment: . env/bin/activate
5. install the requirements: pip install -r requirements.txt
6. define required environmental variables and store in .env
7. run it in the background with nohup:
 nohup python -u pdf2pgs3.py <first-id> <last-id> >> load.log 2>&1&
