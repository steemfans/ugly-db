FROM ety001/steem-python:es
WORKDIR /app
ADD temp /app/temp
RUN cp -f /app/temp/settings.db.init /app/temp/settings.db
CMD ["/app/temp/run.py"]
