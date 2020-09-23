FROM python:latest
RUN mkdir atlan_collect_interrupt
WORKDIR /atlan_collect_interrupt
COPY requirements.txt /atlan_collect_interrupt
RUN pip install -r requirements.txt
COPY . /atlan_collect_interrupt
EXPOSE 5000
CMD ["python", "-u", "app.py"]