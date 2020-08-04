FROM ety001/steem-python:es
WORKDIR /app
ADD temp /app/temp
CMD ["/app/temp/run.py"]
